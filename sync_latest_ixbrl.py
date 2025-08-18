#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily iXBRL -> Azure SQL (staging append via pandas.to_sql)

- Tries a rolling window (default 1 = today only). With --lookback-days N,
  it checks today..(today-N+1) and appends any that exist.
- 404 daily files are skipped (soft success); other HTTP errors exit 1.
- Uses pandas.to_sql with fast_executemany to avoid the "parameter markers" error.

Requires:
  pip install httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import httpx
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from stream_read_xbrl import stream_read_xbrl_zip


def zip_url_for(date_str: str) -> str:
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{date_str}.zip"


def fetch_zip_as_df(date_str: str, timeout: float = 120.0) -> pd.DataFrame:
    """
    Stream the daily ZIP and parse iXBRL into a DataFrame.
    The parser may print warnings about individual bad files; we keep the good rows.
    """
    url = zip_url_for(date_str)
    print(f"Fetching: {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    df = pd.DataFrame(data, columns=columns)
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Ensure a canonical 'company_number' column (strip spaces)
    - Normalise 'period_end' as datetime (if present under various names)
    """
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = (
            df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )

    # Canonicalise 'period_end'
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    return df


def build_engine(
    *,
    engine_url: Optional[str],
    odbc_connect: Optional[str],
) -> Engine:
    """
    Build a SQLAlchemy engine for SQL Server + pyodbc with fast_executemany.
    You can supply either:
      - engine_url (e.g., mssql+pyodbc:///?odbc_connect=<urlencoded>)
      - odbc_connect (raw ODBC connect string; we'll url-encode and build engine_url)
    """
    if not engine_url and not odbc_connect:
        # Try env vars
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")

    if not engine_url and not odbc_connect:
        raise RuntimeError(
            "No connection info. Provide --engine-url or --odbc-connect, "
            "or set AZURE_SQL_URL or AZURE_SQL_ODBC."
        )

    if not engine_url:
        # url-encode an ODBC connection string
        import urllib.parse
        params = urllib.parse.quote_plus(odbc_connect)
        engine_url = f"mssql+pyodbc:///?odbc_connect={params}"

    # fast_executemany=True is the key to avoid the parameter markers explosion
    eng = create_engine(engine_url, fast_executemany=True, future=True)
    return eng


def infer_staging_dtypes(df: pd.DataFrame) -> Dict[str, "sqlalchemy.types.TypeEngine"]:
    """
    Use simple, safe staging types:
      - object -> NVARCHAR(max)
      - datetime/date -> DATE
      - everything else -> NVARCHAR(max) (keeps staging lenient)
    """
    from sqlalchemy import types as sqltypes

    dtypes: Dict[str, sqltypes.TypeEngine] = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(dtype):
            dtypes[col] = sqltypes.Date()
        elif pd.api.types.is_object_dtype(dtype):
            dtypes[col] = sqltypes.NVARCHAR(length=None)  # NVARCHAR(MAX)
        else:
            # Keep staging permissive. You can tighten later if you want.
            dtypes[col] = sqltypes.NVARCHAR(length=None)
    return dtypes


def to_sql_append(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    schema: Optional[str] = None,
    chunksize: int = 1000,
) -> int:
    """
    Append to staging with pandas.to_sql using fast_executemany.
    Returns number of rows written.
    """
    if df.empty:
        return 0

    # Ensure column names are SQL-friendly
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})

    dtypes = infer_staging_dtypes(df)

    # Write
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",   # auto-create on first run
        index=False,
        dtype=dtypes,
        chunksize=chunksize,
        method=None,          # let SQLAlchemy use executemany; fast_executemany handles perf
    )

    return len(df)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch CH daily ZIP(s) and append to Azure SQL staging.")
    ap.add_argument("--date", help="Fetch specific date (YYYY-MM-DD). If set, only that date.")
    ap.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    ap.add_argument("--lookback-days", type=int, default=1,
                    help="How many days back to try (1=today only, 5=today..-4 days).")
    ap.add_argument("--engine-url", help="SQLAlchemy engine URL (e.g., mssql+pyodbc:///?odbc_connect=...)")
    ap.add_argument("--odbc-connect", help="Raw ODBC connection string (we will url-encode for you).")
    ap.add_argument("--staging-table", default="_stg_fin_2d618a4e",
                    help="Table to append into (defaults to previous staging table).")
    ap.add_argument("--schema", default="dbo", help="Schema to use (default dbo).")

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

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building SQL engine: {e}")
        return 1

    any_success = False

    for date_str in dates:
        # 1) Fetch & parse
        try:
            df = fetch_zip_as_df(date_str, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No daily file for {date_str} (404). Skipping.")
                continue
            print(f"HTTP error for {date_str}: {e}")
            return 1
        except Exception as e:
            # If the parser raised on a particular bad internal file, we still exit here
            print(f"Unexpected error while fetching/parsing {date_str}: {e}")
            return 1

        if df.empty:
            print(f"{date_str}: ZIP parsed but had 0 rows. Skipping.")
            continue

        # 2) Normalise
        df = normalise_columns(df)

        # 3) Append to staging via pandas.to_sql (fast_executemany)
        try:
            rows = to_sql_append(
                df=df,
                engine=engine,
                table=args.staging_table,
                schema=args.schema,
                chunksize=1000,
            )
        except SQLAlchemyError as e:
            print(f"Upsert (staging append) failed for {date_str}: {args.schema}.{args.staging_table}")
            print(f"Error: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected SQL error for {date_str}: {e}")
            return 1

        print(f"✅ {date_str}: appended {rows} rows into {args.schema}.{args.staging_table}.")
        any_success = True

    if not any_success:
        print("No files available in the lookback window. Exiting successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
