#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL

Now supports three ways to choose which months to ingest:
  A) Explicit range: --start-month YYYY-MM --end-month YYYY-MM   (inclusive)
  B) Explicit list:  --months 2024-01,2024-02,2024-03
  C) Rolling lookback by months: --since-months N   (N=1 means current month only)

Default target is dbo.financials.
"""

from __future__ import annotations
import argparse
import os
from datetime import date
from typing import Iterable, List, Optional

import pandas as pd
import httpx
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# --- you likely already have these helpers in your repo; included here for completeness ---
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
            d[c] = T.NVARCHAR(length=None)
        else:
            d[c] = T.NVARCHAR(length=None)
    return d

def to_sql_append(df: pd.DataFrame, engine: Engine, table: str, schema: str = "dbo", chunksize: int = 1000) -> int:
    if df.empty:
        return 0
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    df.to_sql(name=table, con=engine, schema=schema, if_exists="append",
              index=False, dtype=infer_staging_dtypes(df), chunksize=chunksize, method=None)
    return len(df)

# --- monthly helpers ---
def parse_yyyy_mm(s: str) -> date:
    y, m = s.split("-", 1)
    return date(int(y), int(m), 1)

def month_iter_inclusive(start_ym: str, end_ym: str) -> Iterable[str]:
    cur = parse_yyyy_mm(start_ym)
    end = parse_yyyy_mm(end_ym)
    while cur <= end:
        yield f"{cur.year:04d}-{cur.month:02d}"
        # add one month
        y, m = cur.year, cur.month + 1
        if m == 13:
            y, m = y + 1, 1
        cur = date(y, m, 1)

def months_from_list(csv: str) -> List[str]:
    return [x.strip() for x in csv.split(",") if x.strip()]

def default_since_months(n: int) -> List[str]:
    # current month back N-1 months
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).date().replace(day=1)
    out = []
    y, m = today.year, today.month
    for i in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out

# You likely already have this; keep it as your parser for a single monthly ZIP
def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> pd.DataFrame:
    # Example URL pattern; keep your existing implementation if different:
    url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{yyyy_mm}.zip"
    print(f"Fetching month: {yyyy_mm} -> {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        from stream_read_xbrl import stream_read_xbrl_zip
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    return pd.DataFrame(data, columns=columns)

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "companies_house_registered_number" in df.columns and "company_number" not in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date
    return df

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    # NEW: explicit range
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    # Existing/legacy options
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials", help="Final table (default: financials)")
    ap.add_argument("--timeout", type=float, default=180.0)

    args = ap.parse_args()

    # Work out the month set (priority: explicit range > explicit list > rolling)
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    print(f"Target months: {months}")

    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    wrote_any = False

    for ym in months:
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

        df = normalise_columns(df)

        try:
            rows = to_sql_append(df, engine, table=args.target_table, schema=args.schema, chunksize=1000)
        except SQLAlchemyError as e:
            print(f"SQL error for {ym} into {args.schema}.{args.target_table}: {e}")
            return 1

        print(f"✅ {ym}: appended {rows} rows into {args.schema}.{args.target_table}.")
        wrote_any = True

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
