#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL

Choose months via:
  A) Explicit range: --start-month YYYY-MM --end-month YYYY-MM   (inclusive)
  B) Explicit list:  --months 2024-01,2024-02
  C) Rolling:        --since-months N  (N=1 = current month only)

URLs handled:
  - 2025+                          -> https://download.companieshouse.gov.uk/Accounts_Monthly_Data-MonthYYYY.zip
  - 2024 Jan..Jun                  -> https://download.companieshouse.gov.uk/Accounts_Monthly_Data-Month2024.zip
  - 2024 Jul..Dec and 2010..2023   -> https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-MonthYYYY.zip
  - 2009                           -> https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember2009.zip
  - 2008                           -> https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember2008.zip

Default target is dbo.financials.
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError


# --------------------------
# SQL helpers
# --------------------------
def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    """
    Build SQLAlchemy engine for SQL Server/pyodbc.
    Accepts either:
      - engine_url (mssql+pyodbc:///?odbc_connect=URLENCODED)
      - odbc_connect (raw ODBC connect string; we URL-encode here)
    """
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")

    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")

    if not engine_url:
        import urllib.parse

        # ensure a sane connection timeout unless already set
        if "Connection Timeout=" not in odbc_connect and "Timeout=" not in odbc_connect:
            if not odbc_connect.endswith(";"):
                odbc_connect += ";"
            odbc_connect += "Connection Timeout=60;"

        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    return create_engine(engine_url, fast_executemany=True, future=True)


def infer_staging_dtypes(df: pd.DataFrame):
    from sqlalchemy import types as T

    d = {}
    for c, t in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(t):
            d[c] = T.Date()
        elif pd.api.types.is_object_dtype(t):
            d[c] = T.NVARCHAR(length=None)  # NVARCHAR(MAX)
        else:
            d[c] = T.NVARCHAR(length=None)
    return d


def to_sql_append_with_retry(
    df: pd.DataFrame,
    engine_factory,
    schema: str,
    table: str,
    chunksize: int = 1000,
    max_attempts: int = 5,
    sleep_seconds: int = 8,
) -> int:
    """
    Append df into schema.table with retries around transient ODBC 'HYT00' login timeouts.
    engine_factory: callable returning a fresh Engine (so we can recreate on retry).
    """
    if df.empty:
        return 0

    # SQL-friendly columns
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    dtypes = infer_staging_dtypes(df)

    attempt = 0
    last_err: Optional[BaseException] = None
    while attempt < max_attempts:
        attempt += 1
        try:
            eng: Engine = engine_factory()
            with eng.begin() as conn:
                df.to_sql(
                    name=table,
                    con=conn,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    dtype=dtypes,
                    chunksize=chunksize,
                    method=None,
                )
            return len(df)
        except OperationalError as e:
            # Typical transient: ('HYT00', ... Login timeout expired)
            last_err = e
            msg = str(e).lower()
            print(f"[append attempt {attempt}/{max_attempts}] OperationalError: {e}")
            if attempt < max_attempts and ("hyt00" in msg or "timeout" in msg or "temporar" in msg):
                time.sleep(sleep_seconds)
                continue
            raise
        except Exception as e:
            last_err = e
            print(f"[append attempt {attempt}/{max_attempts}] Unexpected SQL error: {e}")
            raise
    if last_err:
        raise last_err
    return 0


# --------------------------
# Month selection helpers
# --------------------------
MONTH_NAMES = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def parse_yyyy_mm(s: str) -> date:
    y, m = s.split("-", 1)
    return date(int(y), int(m), 1)


def month_iter_inclusive(start_ym: str, end_ym: str) -> Iterable[str]:
    cur = parse_yyyy_mm(start_ym)
    end = parse_yyyy_mm(end_ym)
    while cur <= end:
        yield f"{cur.year:04d}-{cur.month:02d}"
        y, m = cur.year, cur.month + 1
        if m == 13:
            y, m = y + 1, 1
        cur = date(y, m, 1)


def months_from_list(csv: str) -> List[str]:
    return [x.strip() for x in csv.split(",") if x.strip()]


def default_since_months(n: int) -> List[str]:
    # current month back N-1 months
    today = datetime.now(timezone.utc).date().replace(day=1)
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def ym_to_month_name(ym: str) -> Tuple[str, int, int]:
    """Return (MonthName, year, month) for 'YYYY-MM'."""
    d = parse_yyyy_mm(ym)
    return MONTH_NAMES[d.month - 1], d.year, d.month


def monthly_zip_url(ym: str) -> str:
    """
    Build the correct CH monthly ZIP URL for a given 'YYYY-MM'.
    Rules per user examples:
      - 2025+  -> non-archive
      - 2024   -> Jan–Jun non-archive; Jul–Dec archive
      - 2010–2023 -> archive
      - 2009, 2008 -> special 'JanuaryToDecember' archives
    """
    month_name, year, month = ym_to_month_name(ym)

    # Special yearly bundles
    if year == 2009 or year == 2008:
        return (
            "https://download.companieshouse.gov.uk/archive/"
            f"Accounts_Monthly_Data-JanuaryToDecember{year}.zip"
        )

    base = "https://download.companieshouse.gov.uk"
    path = f"/Accounts_Monthly_Data-{month_name}{year}.zip"

    # Archive switch
    use_archive = False
    if 2010 <= year <= 2023:
        use_archive = True
    elif year == 2024 and month >= 7:
        use_archive = True
    elif year <= 2007:
        use_archive = True  # (defensive; unlikely needed)

    if use_archive:
        return f"{base}/archive{path}"
    else:
        return f"{base}{path}"


# --------------------------
# Fetch + normalise
# --------------------------
def fetch_month_zip_as_df(ym: str, timeout: float = 180.0) -> pd.DataFrame:
    url = monthly_zip_url(ym)
    print(f"Fetching month: {ym} -> {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        from stream_read_xbrl import stream_read_xbrl_zip

        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    return pd.DataFrame(data, columns=columns)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "companies_house_registered_number" in df.columns and "company_number" not in df.columns:
        df["company_number"] = (
            df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date
    return df


# --------------------------
# Main
# --------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    # Range
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    # List / Lookback
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials", help="Final table (default: financials)")
    ap.add_argument("--timeout", type=float, default=180.0)

    args = ap.parse_args()

    # Decide month set
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    print(f"Target months: {months}")

    # Engine factory so we can recreate between retries
    def engine_factory() -> Engine:
        return build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)

    # Smoke-test engine once up front (prints clearer error early)
    try:
        eng = engine_factory()
        with eng.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
    except Exception as e:
        print(f"ERROR building/connecting engine: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # 1) Fetch + parse
        try:
            df = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No monthly file for {ym} (404). Skipping.")
                continue
            print(f"HTTP error for {ym}: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected fetch/parse error for {ym}: {e}")
            return 1

        if df.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # 2) Normalise
        df = normalise_columns(df)

        # 3) Append (with transient timeout retries)
        try:
            rows = to_sql_append_with_retry(
                df=df,
                engine_factory=engine_factory,
                schema=args.schema,
                table=args.target_table,
                chunksize=1000,
                max_attempts=5,
                sleep_seconds=8,
            )
        except SQLAlchemyError as e:
            print(f"SQL error for {ym} into {args.schema}.{args.target_table}: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected SQL error for {ym}: {e}")
            return 1

        print(f"✅ {ym}: appended {rows} rows into {args.schema}.{args.target_table}.")
        wrote_any = True

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
