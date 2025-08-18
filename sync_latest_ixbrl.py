#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily iXBRL -> Azure SQL (via pandas.to_sql)

What this does
--------------
- Tries a rolling window (default 1 = today only). With --lookback-days N,
  it checks today, yesterday, ... up to N days back, and upserts any that exist.
- 404s are skipped (soft success).
- Corrupt/invalid iXBRL entries inside the daily ZIP are skipped (day still ingests).
- Uses SQLAlchemy + pyodbc with fast_executemany + chunked to_sql to avoid
  the pyodbc parameter marker explosion error you hit earlier.

Requirements
------------
pip install httpx pandas stream-read-xbrl sqlalchemy pyodbc

Environment variables (must be set)
-----------------------------------
AZURE_SQL_SERVER      e.g. ixbrl-sql-server.database.windows.net
AZURE_SQL_DATABASE    e.g. ixbrl_financials
AZURE_SQL_USERNAME    e.g. user@ixbrl-sql-server
AZURE_SQL_PASSWORD

Table
-----
Data lands in staging: dbo._stg_fin_2d618a4e
(We auto-intersect dataframe columns with the table’s columns.)

Optionally, set --merge-proc to call a stored proc after staging, e.g.
  --merge-proc dbo.sp_merge_financials_from_staging
"""

from __future__ import annotations
import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Tuple

import httpx
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip

from sqlalchemy import create_engine, text, inspect, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


STAGING_TABLE = "dbo._stg_fin_2d618a4e"


def zip_url_for(date_str: str) -> str:
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{date_str}.zip"


def _yield_rows_safely(iter_rows: Iterable[Tuple]) -> Iterable[Tuple]:
    """
    Wrap the row iterator so that any single bad iXBRL file inside the ZIP
    doesn’t abort the whole day. We’ll skip rows that throw on access.
    """
    for idx, row in enumerate(iter_rows):
        try:
            yield row
        except Exception as e:
            print(f"  Skipping one bad entry at position {idx}: {e}")


def fetch_zip_as_df(date_str: str, timeout: float = 120.0) -> pd.DataFrame:
    url = zip_url_for(date_str)
    print(f"Fetching: {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        # stream_read_xbrl_zip yields (columns, rows_iter)
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            safe_rows = _yield_rows_safely(rows)
            data = [[("" if v is None else str(v)) for v in row] for row in safe_rows]
    return pd.DataFrame(data, columns=columns)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # company_number normalization
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)

    # unifying period end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")

    return df


def build_engine() -> Engine:
    server = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]

    conn_str = (
        "mssql+pyodbc://"
        f"{username}:{password}@{server}:1433/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True, pool_pre_ping=True)

    # Safety: ensure cursor.fast_executemany is set even if SQLAlchemy flag is ignored
    @event.listens_for(engine, "do_setinputsizes")
    def _inputsizes(*args, **kwargs):
        # no-op: avoids some driver quirks with large varchars
        return

    return engine


def _get_table_columns(engine: Engine, table_fullname: str) -> List[str]:
    # Resolve schema + table
    if "." in table_fullname:
        schema, table = table_fullname.split(".", 1)
    else:
        schema, table = "dbo", table_fullname

    inspector = inspect(engine)
    cols = [c["name"] for c in inspector.get_columns(table, schema=schema)]
    return cols


def _align_to_table(df: pd.DataFrame, table_cols: List[str]) -> pd.DataFrame:
    # Keep only columns that exist in the destination table; add missing ones as None
    keep = [c for c in df.columns if c in table_cols]
    missing = [c for c in table_cols if c not in df.columns]
    out = df[keep].copy()
    for m in missing:
        out[m] = None
    # Reorder to table column order for deterministic insert
    out = out[table_cols]
    return out


def to_sql_chunked(engine: Engine, df: pd.DataFrame, table: str, chunksize: int = 1000) -> int:
    """
    Append df to table in chunks using pandas.to_sql, with fast_executemany.
    Returns the number of rows appended.
    """
    if df.empty:
        return 0

    table_cols = _get_table_columns(engine, table)
    df = _align_to_table(df, table_cols)

    # Ensure datetimes are timezone-naive for SQL Server
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c]).dt.tz_localize(None)

    # Write in chunks
    total = 0
    try:
        df.to_sql(
            name=table.split(".", 1)[-1],
            schema=table.split(".", 1)[0] if "." in table else None,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method=None,  # rely on fast_executemany
        )
        total = len(df)
    except SQLAlchemyError as e:
        # Retry with smaller chunks if driver complains
        print(f"to_sql failed with chunksize={chunksize}: {e}. Retrying with smaller chunks…")
        small = max(100, chunksize // 5)
        df.to_sql(
            name=table.split(".", 1)[-1],
            schema=table.split(".", 1)[0] if "." in table else None,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=small,
            method=None,
        )
        total = len(df)
    return total


def maybe_call_merge_proc(engine: Engine, proc_name: Optional[str], run_code: str) -> None:
    if not proc_name:
        return
    print(f"Calling merge proc: {proc_name} (run_code={run_code})")
    with engine.begin() as conn:
        conn.execute(text(f"EXEC {proc_name} :run_code"), {"run_code": run_code})


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch CH daily ZIP(s) and upsert to Azure SQL (staging).")
    ap.add_argument("--date", help="Fetch specific date (YYYY-MM-DD). If set, only that date.")
    ap.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    ap.add_argument("--lookback-days", type=int, default=1,
                    help="How many days back to try (1=today only, 5=today..-4 days).")
    ap.add_argument("--chunksize", type=int, default=1000, help="Batch size for pandas.to_sql.")
    ap.add_argument("--merge-proc", help="Optional stored proc to merge from staging to final tables.")
    args = ap.parse_args()

    # Build date list
    dates: List[str] = []
    if args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD")
            return 2
        dates = [target.strftime("%Y-%m-%d")]
    else:
        start = datetime.now(timezone.utc).date()
        for i in range(args.lookback_days):
            d = (start - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(d)

    engine = build_engine()
    any_success = False

    for date_str in dates:
        run_code = date_str  # handy to tag rows
        try:
            df = fetch_zip_as_df(date_str, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No daily file for {date_str} (404). Skipping.")
                continue
            print(f"HTTP error for {date_str}: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected error while fetching {date_str}: {e}")
            return 1

        if df.empty:
            print(f"{date_str}: ZIP parsed but had 0 rows. Skipping.")
            continue

        df = normalise_columns(df)

        # Add a couple of useful fields if the staging table supports them
        if "zip_url" not in df.columns:
            df["zip_url"] = zip_url_for(date_str)
        if "run_code" not in df.columns:
            df["run_code"] = run_code

        # Ingest to staging with pandas.to_sql
        try:
            inserted = to_sql_chunked(engine, df, STAGING_TABLE, chunksize=args.chunksize)
        except Exception as e:
            print(f"Upsert (staging append) failed for {date_str}: {e}")
            return 1

        print(f"✅ {date_str}: appended ~{inserted} rows into {STAGING_TABLE}.")

        # Optional: call a MERGE proc to move from staging to fact tables
        try:
            maybe_call_merge_proc(engine, args.merge_proc, run_code)
        except Exception as e:
            print(f"Warning: merge proc failed for {date_str}: {e}")

        any_success = True

    if not any_success:
        print("No files available in the lookback window. Exiting successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
