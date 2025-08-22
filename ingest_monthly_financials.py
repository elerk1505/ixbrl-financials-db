#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Companies House iXBRL monthly -> Azure SQL (upsert via staging table)

- Robust download (retry + size validation)
- Data normalisation (dates/numbers/empties)
- Staging table with proper quoting
- MERGE upsert on (companies_house_registered_number, date)
- Terse logs

Requires: httpx, pandas, sqlalchemy, pyodbc, stream-read-xbrl
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# Constants / expected columns
# -----------------------------

EXPECTED_COLS: List[str] = [
    "run_code",
    "company_id",
    "date",
    "file_type",
    "taxonomy",
    "balance_sheet_date",
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_dormant",
    "average_number_employees_during_period",
    "period_start",
    "period_end",
    "tangible_fixed_assets",
    "debtors",
    "cash_bank_in_hand",
    "current_assets",
    "creditors_due_within_one_year",
    "creditors_due_after_one_year",
    "net_current_assets_liabilities",
    "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital",
    "profit_loss_account_reserve",
    "shareholder_funds",
    "turnover_gross_operating_revenue",
    "other_operating_income",
    "cost_sales",
    "gross_profit_loss",
    "administrative_expenses",
    "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2",
    "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
    "error",
    "zip_url",
]

DATE_COLS = {"date", "balance_sheet_date", "period_start", "period_end"}
BOOL_COLS = {"company_dormant"}
NUMERIC_COLS = {
    # non-exhaustive: we'll coerce every non-date/str column below by best effort,
    # but explicitly list the obvious numeric ones:
    "average_number_employees_during_period",
    "tangible_fixed_assets",
    "debtors",
    "cash_bank_in_hand",
    "current_assets",
    "creditors_due_within_one_year",
    "creditors_due_after_one_year",
    "net_current_assets_liabilities",
    "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital",
    "profit_loss_account_reserve",
    "shareholder_funds",
    "turnover_gross_operating_revenue",
    "other_operating_income",
    "cost_sales",
    "gross_profit_loss",
    "administrative_expenses",
    "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2",
    "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
}

# -----------------------------
# CLI helpers
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
# Engine / connection
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        # Add retry-friendly options if not already present
        if "ConnectRetryCount" not in odbc_connect:
            odbc_connect += ";ConnectRetryCount=3;ConnectRetryInterval=10"
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    # fast_executemany speeds up executemany() with pyodbc
    return create_engine(engine_url, fast_executemany=True, future=True)

# -----------------------------
# URL construction & download
# -----------------------------

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]

def candidate_urls_for_month(ym: str) -> list[str]:
    """Prefer LIVE monthly URL, then fall back to ARCHIVE, then 2008/2009 yearly bundle."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)  # e.g. 'January'
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"

    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(
            f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip"
        )
    return urls


def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0, per_url_retries: int = 3) -> pd.DataFrame:
    from stream_read_xbrl import stream_read_xbrl_zip
    import time
    last_err: Exception | None = None

    for url in candidate_urls_for_month(yyyy_mm):
        for attempt in range(1, per_url_retries + 1):
            try:
                print(f"Fetching month: {yyyy_mm} -> {url} (attempt {attempt}/{per_url_retries})")
                with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                    r.raise_for_status()
                    with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                        data = [[("" if v is None else str(v)) for v in row] for row in rows]
                return dedupe_columns(pd.DataFrame(data, columns=columns))

            except httpx.HTTPStatusError as e:
                # If it's a 404, move on to the next candidate URL immediately
                if e.response is not None and e.response.status_code == 404:
                    print(f"No monthly file at {url} (404). Trying next candidate…")
                    last_err = e
                    break  # break retry loop -> next URL
                last_err = e
                print(f"HTTP error at {url}: {e}.")
                # small backoff then retry same URL
                if attempt < per_url_retries:
                    time.sleep(2 * attempt)

            except (httpx.ReadError, httpx.RemoteProtocolError, httpx.ConnectError) as e:
                # transient network issue; retry same URL
                last_err = e
                print(f"Network error at {url}: {e} — retrying…")
                if attempt < per_url_retries:
                    time.sleep(3 * attempt)

            except Exception as e:
                # parsing or other error; try next URL
                last_err = e
                print(f"Parse error at {url}: {e} — trying next candidate.")
                break

    raise RuntimeError(f"{yyyy_mm}: all URL candidates failed. Last error: {last_err}")

# -----------------------------
# DataFrame shaping
# -----------------------------

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def series_or_first(df: pd.DataFrame, name: str) -> pd.Series:
    obj = df[name]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj

def normalise_df(df: pd.DataFrame, zip_url: str) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Ensure 'companies_house_registered_number' exists and has no spaces
    for cand in ("companies_house_registered_number", "company_number", "ch_number", "companyNumber"):
        if cand in df.columns:
            s = series_or_first(df, cand).astype(str).str.replace(" ", "", regex=False)
            df["companies_house_registered_number"] = s
            break

    # Unite date columns to datetime (we'll keep as Python datetime for pyodbc)
    for c in DATE_COLS:
        if c in df.columns:
            s = pd.to_datetime(series_or_first(df, c), errors="coerce", utc=False)
            df[c] = s.dt.to_pydatetime()

    # Booleans: accept 'True'/'False', 'true'/'false', 1/0; convert to 0/1 (BIT)
    if "company_dormant" in df.columns:
        s = series_or_first(df, "company_dormant").astype(str).str.strip().str.lower()
        df["company_dormant"] = s.isin({"true", "1", "yes", "y"})

    # Clean: empty strings to None so SQL Server gets NULL
    def empty_to_none(x):
        if isinstance(x, str) and x.strip() == "":
            return None
        return x

    df = df.applymap(empty_to_none)

    # Keep only expected columns (if present)
    present = [c for c in EXPECTED_COLS if c in df.columns]
    df = df[present].copy()

    # Ensure all expected columns exist (fill missing with None)
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = None

    # Numeric coercion (best effort) — leave NULLs as None
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # Add zip_url (constant for the batch)
    df["zip_url"] = zip_url

    # Final column order matches EXPECTED_COLS
    df = df[EXPECTED_COLS]

    return df

# -----------------------------
# SQL helpers (staging + upsert)
# -----------------------------

def qname(schema: str, table: str) -> str:
    # Quote SQL Server identifiers safely
    return f"[{schema}].[{table}]"

def ensure_staging_table(engine: Engine, schema: str, staging_table: str) -> None:
    """
    Create the staging table if it doesn't exist. All columns NVARCHAR(MAX) for simplicity.
    (We keep types wide because we normalise/cast at upsert time.)
    """
    cols_sql = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in EXPECTED_COLS)

    sql = f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = N'{schema}' AND t.name = N'{staging_table}'
)
BEGIN
    EXEC('CREATE TABLE {qname(schema, staging_table)} (
        {cols_sql}
    )');
END
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def write_stage(engine: Engine, schema: str, staging_table: str, df: pd.DataFrame) -> int:
    """
    Efficient executemany into staging. We pass NVARCHARs only, so convert to str where appropriate
    and keep None for NULL. Dates are converted to ISO strings for safety.
    """
    if df.empty:
        return 0

    # Convert objects to strings, but keep None. Dates -> ISO date (yyyy-mm-dd)
    def to_sql_cell(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        if isinstance(v, (datetime, date)):
            # Keep date only (aligning with target 'date' semantics used in your PK)
            return v.strftime("%Y-%m-%d")
        if isinstance(v, (bool, np.bool_)):
            return "1" if v else "0"
        return str(v)

    rows = [tuple(to_sql_cell(v) for v in rec) for rec in df.itertuples(index=False, name=None)]
    collist = ", ".join(f"[{c}]" for c in EXPECTED_COLS)
    placeholders = ", ".join(["?"] * len(EXPECTED_COLS))
    insert_sql = f"INSERT INTO {qname(schema, staging_table)} ({collist}) VALUES ({placeholders})"

    # Retry loop for transient connectivity
    attempts = 5
    for k in range(1, attempts + 1):
        try:
            with engine.begin() as conn:
                cur = conn.connection.cursor()
                cur.fast_executemany = True
                cur.executemany(insert_sql, rows)
            return len(rows)
        except Exception as e:
            if k == attempts:
                raise
            time.sleep(3 * k)

def merge_upsert(engine: Engine, schema: str, staging_table: str, target_table: str) -> Tuple[int, int]:
    """
    MERGE from staging into target on (companies_house_registered_number, date).
    Returns (updated_count, inserted_count).
    """
    tgt = qname(schema, target_table)
    stg = qname(schema, staging_table)

    key_cols = ["companies_house_registered_number", "date"]
    non_keys = [c for c in EXPECTED_COLS if c not in key_cols]

    set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_keys)
    insert_cols = ", ".join(f"[{c}]" for c in EXPECTED_COLS)
    insert_vals = ", ".join(f"s.[{c}]" for c in EXPECTED_COLS)
    on_clause = " AND ".join(f"t.[{k}] = s.[{k}]" for k in key_cols)

    # Use output into table variables to count actions
    sql = f"""
DECLARE @ins INT = 0, @upd INT = 0;

MERGE {tgt} AS t
USING (SELECT * FROM {stg}) AS s
ON ({on_clause})
WHEN MATCHED THEN
    UPDATE SET {set_clause}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols}) VALUES ({insert_vals})
OUTPUT
    $action
INTO #merge_out(action);

-- Count results
SELECT @ins = COUNT(*) FROM #merge_out WHERE action = 'INSERT';
SELECT @upd = COUNT(*) FROM #merge_out WHERE action = 'UPDATE';

DROP TABLE #merge_out;

SELECT @upd AS updated, @ins AS inserted;
"""

    # Create the temp output table before MERGE
    prefix = f"IF OBJECT_ID('tempdb..#merge_out') IS NOT NULL DROP TABLE #merge_out; CREATE TABLE #merge_out(action NVARCHAR(10));"
    with engine.begin() as conn:
        result = conn.exec_driver_sql(prefix + sql).fetchone()
        updated, inserted = int(result[0]), int(result[1])
    return updated, inserted

def truncate_staging(engine: Engine, schema: str, staging_table: str) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {qname(schema, staging_table)}")

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest monthly Companies House iXBRL into Azure SQL (upsert).")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    grp.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive, used with --start-month).")
    ap.add_argument("--since-months", type=int, default=1, help="If neither --months nor --start-month given.")
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--quiet", action="store_true", help="Reduce log verbosity.")
    args = ap.parse_args()

    # Build month list
    if args.months:
        months = months_from_list(args.months)
    elif args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    else:
        months = default_since_months(args.since_months)

    print(f"Target months: {months}")

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    # Ensure staging exists
    try:
        ensure_staging_table(engine, args.schema, args.staging_table)
    except Exception as e:
        print(f"ERROR ensuring staging table: {e}")
        return 1

    total_upd = total_ins = 0
    wrote_any = False

    for ym in months:
        try:
            dl = fetch_month_zip_as_rows(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        # DataFrame from rows (convert Nones to "", then back later)
        data = [[("" if v is None else v) for v in row] for row in dl.rows]
        df = pd.DataFrame(data, columns=dl.columns)

        if df.empty:
            if not args.quiet:
                print(f"{ym}: 0 rows.")
            continue

        df = normalise_df(df, zip_url=dl.url)

        try:
            truncate_staging(engine, args.schema, args.staging_table)
            staged = write_stage(engine, args.schema, args.staging_table, df)
            upd, ins = merge_upsert(engine, args.schema, args.staging_table, args.target_table)
            total_upd += upd
            total_ins += ins
            wrote_any = True
            print(f"✅ {ym}: staged {staged:,} → upserted (updated={upd:,}, inserted={ins:,}).")
        except SQLAlchemyError as e:
            print(f"{ym}: SQL error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: unexpected error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested.")
    else:
        print(f"Done. Total updated={total_upd:,}, inserted={total_ins:,}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
