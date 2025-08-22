#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Companies House MONTHLY iXBRL -> Azure SQL

- Month selection: --start-month/--end-month | --months | --since-months
- Robust download with retry + Content-Length validation
- Stage (NVARCHAR) -> Insert into target with TRY_CONVERT for dates/numbers
- Skip duplicates using NOT EXISTS on (companies_house_registered_number, [date])
"""

from __future__ import annotations
import argparse
import os
import io
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# Helpers: month handling
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
# CH URLs
# -----------------------------

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # e.g. January

def candidate_urls_for_month(ym: str) -> List[str]:
    """Return URL candidates from newest to oldest."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls

# -----------------------------
# HTTP download with verification
# -----------------------------

def download_bytes(url: str, timeout: float, max_attempts: int = 5, sleep_secs: float = 5.0) -> bytes:
    for attempt in range(1, max_attempts + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length") or 0)
                buf = io.BytesIO()
                for chunk in r.iter_bytes():
                    if chunk:
                        buf.write(chunk)
                data = buf.getvalue()
                if expected and len(data) != expected:
                    raise IOError(f"incomplete body: got {len(data)} expected {expected}")
                return data
        except httpx.HTTPStatusError as e:
            # Only retry non-404s; 404 means move to next candidate URL
            if e.response is not None and e.response.status_code == 404:
                raise
            print(f"  download attempt {attempt}/{max_attempts} failed: {e}")
        except Exception as e:
            print(f"  download attempt {attempt}/{max_attempts} failed: {e}")
        time.sleep(sleep_secs)
    raise IOError("download failed after retries")

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> pd.DataFrame:
    from stream_read_xbrl import stream_read_xbrl_zip
    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            blob = download_bytes(url, timeout=timeout)
            # parse from in-memory bytes with the streaming parser
            with stream_read_xbrl_zip(iter([blob])) as (columns, rows):
                data = [[("" if v is None else str(v)) for v in row] for row in rows]
            df = pd.DataFrame(data, columns=columns)
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()].copy()
            # remember where this row came from
            df["zip_url"] = url
            return df
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"  404 at {url}, trying next URL…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            print(f"  fetch/parse error: {e}")
            break
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# -----------------------------
# Normalisation
# -----------------------------

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Align company number
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Canonical date column name -> "date" (source has "period_end"/"balance_sheet_date")
    # We keep original columns too; conversion happens in SQL with TRY_CONVERT.
    if "date" not in df.columns:
        for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
            if cand in df.columns:
                df = df.rename(columns={cand: "date"})
                break

    # Make booleans/numbers safe as strings; empty becomes ''
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]) or pd.api.types.is_string_dtype(df[c]):
            continue
        df[c] = df[c].astype(str)

    # standardise header spacing
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    return df

# -----------------------------
# SQL: engine + utilities
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        # resiliency knobs in the connection string help with transient issues
        if "ConnectRetryCount" not in odbc_connect:
            odbc_connect += ";ConnectRetryCount=3;ConnectRetryInterval=5"
        if "Connection Timeout" not in odbc_connect and "ConnectionTimeout" not in odbc_connect:
            odbc_connect += ";Connection Timeout=60"
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    return create_engine(engine_url, fast_executemany=True, future=True)

def qident(name: str) -> str:
    # Quote an identifier for T‑SQL
    return "[" + name.replace("]", "]]") + "]"

def ensure_staging_table(engine: Engine, schema: str, staging: str, target_cols: Sequence[str]) -> None:
    """Create NVARCHAR(MAX) staging table if missing."""
    cols_sql = ",\n".join(f"{qident(c)} NVARCHAR(MAX) NULL" for c in target_cols)
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = N'{schema}' AND t.name = N'{staging}'
)
BEGIN
    EXEC(N'CREATE TABLE {qident(schema)}.{qident(staging)} (
        {cols_sql}
    )');
END
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def truncate_staging(engine: Engine, schema: str, staging: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {qident(schema)}.{qident(staging)}")

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]

# -----------------------------
# Stage -> Target insert with conversions & de-dupe
# -----------------------------

NUMERIC_COLS = {
    "average_number_employees_during_period",
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year",
    "net_current_assets_liabilities", "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
    "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2",
    "operating_profit_loss", "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period"
}
DATE_COLS = {"date", "balance_sheet_date", "period_start", "period_end"}

def conv_expr(col: str) -> str:
    c = qident(col)
    if col in DATE_COLS:
        return f"TRY_CONVERT(date, NULLIF({c}, ''))"
    if col in NUMERIC_COLS:
        # float is permissive and will cast to DECIMAL/NUMERIC on insert
        return f"TRY_CONVERT(float, NULLIF({c}, ''))"
    if col.lower() == "company_dormant":
        return f"TRY_CONVERT(bit, NULLIF({c}, ''))"
    # strings
    return c

def insert_new_rows(engine: Engine, schema: str, staging: str, target: str, target_cols: Sequence[str]) -> int:
    # Build SELECT list with conversions per column
    sel_list = ", ".join(conv_expr(c) for c in target_cols)
    cols_list = ", ".join(qident(c) for c in target_cols)

    # Use NOT EXISTS on (companies_house_registered_number, [date]) to skip PK duplicates
    # If either column is missing, fall back to LEFT JOIN/IS NULL
    match_cols = {"companies_house_registered_number", "date"}
    if not match_cols.issubset(set(target_cols)):
        raise RuntimeError("Target table must contain companies_house_registered_number and date columns.")

    sql = f"""
INSERT INTO {qident(schema)}.{qident(target)} ({cols_list})
SELECT {sel_list}
FROM {qident(schema)}.{qident(staging)} s
WHERE NOT EXISTS (
    SELECT 1
    FROM {qident(schema)}.{qident(target)} t
    WHERE t.{qident('companies_house_registered_number')} = s.{qident('companies_house_registered_number')}
      AND t.{qident('date')} = TRY_CONVERT(date, NULLIF(s.{qident('date')}, ''))
);
SELECT @@ROWCOUNT AS inserted;
"""
    with engine.begin() as conn:
        res = conn.exec_driver_sql(sql)
        # SQLAlchemy returns a cursor-like object; fetch scalar rowcount via first()
        try:
            row = res.fetchone()
            return int(row[0]) if row is not None else 0
        except Exception:
            # Fallback if driver doesn't return @@ROWCOUNT
            return res.rowcount if res.rowcount is not None else 0

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    # Month selection
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list.")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
    # Network / resiliency
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--max-fetch-attempts", type=int, default=5)

    args = ap.parse_args()

    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    print(f"*** --schema {args.schema} --target-table {args.target_table}")
    print(f"Target months: {months}")

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    # Discover target columns
    try:
        target_cols = get_table_columns(engine, args.schema, args.target_table)
        if not target_cols:
            print(f"ERROR: target table {args.schema}.{args.target_table} not found or has no columns.")
            return 1
    except Exception as e:
        print(f"ERROR inspecting target: {e}")
        return 1

    # Make sure staging exists and is clean before each month
    try:
        ensure_staging_table(engine, args.schema, args.staging_table, target_cols)
    except Exception as e:
        print(f"ERROR ensuring staging table: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # fetch & parse
        try:
            df = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        if df.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # normalise columns and align to target schema
        df = normalise_columns(df)
        keep = [c for c in df.columns if c in set(target_cols)]
        df = df[keep].copy()

        # write to staging (transaction per month; guaranteed rollback on error)
        try:
            truncate_staging(engine, args.schema, args.staging_table)
        except Exception as e:
            print(f"{ym}: staging cleanup error: {e}")
            return 1

        try:
            with engine.begin() as conn:
                # use fast_executemany via pandas; all NVARCHAR(MAX) in stage
                df.to_sql(
                    name=args.staging_table,
                    con=conn,
                    schema=args.schema,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method=None,
                )
        except SQLAlchemyError as e:
            print(f"{ym}: staging SQL error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: staging unexpected error: {e}")
            return 1

        # insert new rows into target with conversions + de-dupe
        try:
            inserted = insert_new_rows(engine, args.schema, args.staging_table, args.target_table, target_cols)
            print(f"✅ {ym}: inserted {inserted} new rows into {args.schema}.{args.target_table}.")
            if inserted > 0:
                wrote_any = True
        except SQLAlchemyError as e:
            print(f"{ym}: merge/insert SQL error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: merge/insert unexpected error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested (all months empty or all rows already present).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
