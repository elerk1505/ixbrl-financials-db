#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL

Choose months via:
  A) Range  : --start-month YYYY-MM --end-month YYYY-MM  (inclusive)
  B) List   : --months 2024-01,2024-02
  C) Lookback: --since-months N (N=1 => current month only)

Writes into the requested table (default dbo.financials). Before writing, the
DataFrame is aligned to the table's columns to prevent "Invalid column name" errors.

Also supports the real Monthly ZIP URL patterns:
  - Live    : https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{Month}{YYYY}.zip
  - Archive : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{Month}{YYYY}.zip
  - Special : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{YYYY}.zip  (2008, 2009)

Requires: httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import os
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# SQL helpers
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
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

def to_sql_append(df: pd.DataFrame, engine: Engine, table: str, schema: str = "dbo", chunksize: int = 1000) -> int:
    if df.empty:
        return 0
    # Make column names SQL-friendly
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        dtype=infer_staging_dtypes(df),
        chunksize=chunksize,
        method=None,
    )
    return len(df)

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
    return cols

def align_df_to_table(df: pd.DataFrame, table_columns: Sequence[str]) -> pd.DataFrame:
    cols_set = set(table_columns)

    # If table uses 'companies_house_registered_number', map DF's 'company_number' into it.
    if "company_number" in df.columns and "companies_house_registered_number" in cols_set:
        df = df.rename(columns={"company_number": "companies_house_registered_number"})

    # Strip spaces from company number columns if present
    for cname in ("companies_house_registered_number", "company_number"):
        if cname in df.columns:
            df[cname] = df[cname].astype(str).str.replace(" ", "", regex=False)

    # Keep only columns that actually exist in the table
    keep = [c for c in df.columns if c in cols_set]
    filtered = df[keep].copy()

    return filtered

# -----------------------------
# Month selection helpers
# -----------------------------

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

# -----------------------------
# Download & parse the monthly ZIP
# -----------------------------

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # 'January', 'February', ...

def candidate_urls_for_month(ym: str) -> List[str]:
    """Return URL candidates from newest to oldest locations."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"

    urls = [live, archive]

    # Special case for 2008/2009 yearly bundles
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")

    return urls

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> pd.DataFrame:
    from stream_read_xbrl import stream_read_xbrl_zip

    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                    data = [[("" if v is None else str(v)) for v in row] for row in rows]
            return pd.DataFrame(data, columns=columns)
        except httpx.HTTPStatusError as e:
            # Try the next candidate on 404; re-raise other HTTP errors
            if e.response is not None and e.response.status_code == 404:
                print(f"No monthly file at {url} (404). Trying next candidate…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            break

    # If we got here, nothing worked
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# -----------------------------
# Normalisation
# -----------------------------

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Create canonical 'company_number' if needed
    if "company_number" not in df.columns and "companies_house_registered_number" in df.columns:
        df["company_number"] = (
            df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )
    elif "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)

    # Canonicalise 'period_end'
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    return df

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    # Range / list / lookback
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials", help="Final table (default: financials)")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--append-retries", type=int, default=5, help="Retries for SQL append on transient errors.")

    args = ap.parse_args()

    # Build month list
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    print(f"Target months: {months}")

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    # Determine target table columns once
    try:
        target_cols = get_table_columns(engine, args.schema, args.target_table)
        if not target_cols:
            print(f"ERROR: target table {args.schema}.{args.target_table} not found or has no columns.")
            return 1
        print(f"Detected {args.schema}.{args.target_table} columns: {len(target_cols)}")
    except Exception as e:
        print(f"ERROR inspecting {args.schema}.{args.target_table}: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # 1) Fetch & parse
        try:
            df = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
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

        # 3) Align to target table columns
        df_aligned = align_df_to_table(df, target_cols)
        if df_aligned.empty:
            print(f"{ym}: no overlapping columns with {args.schema}.{args.target_table}. Skipping.")
            continue

        # 4) Append with retries (handles intermittent SQL connectivity)
        last_err: Optional[Exception] = None
        for attempt in range(1, args.append_retries + 1):
            try:
                rows = to_sql_append(df_aligned, engine, table=args.target_table, schema=args.schema, chunksize=1000)
                print(f"✅ {ym}: appended {rows} rows into {args.schema}.{args.target_table}.")
                wrote_any = True
                last_err = None
                break
            except SQLAlchemyError as e:
                last_err = e
                print(f"[append attempt {attempt}/{args.append_retries}] OperationalError: {e}")
            except Exception as e:
                last_err = e
                print(f"[append attempt {attempt}/{args.append_retries}] Unexpected SQL error: {e}")
            # brief backoff
            import time
            time.sleep(5)

        if last_err:
            print(f"SQL error for {ym} into {args.schema}.{args.target_table}: {last_err}")
            return 1

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
