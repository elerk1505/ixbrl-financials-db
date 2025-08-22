#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ingest_monthly_financials.py
Robust end-to-end Companies House monthly-accounts ZIP ingest -> Azure SQL.

Usage examples:
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 --odbc-connect "<ODBC CS>" --schema dbo --target-table financials
  python ingest_monthly_financials.py --months 2025-01 2025-02 --odbc-connect "<ODBC CS>" --schema dbo --target-table financials
  python ingest_monthly_financials.py --since-months 1 --odbc-connect "<ODBC CS>" --schema dbo --target-table financials
"""

import argparse
import datetime as dt
import io
import os
import re
import sys
import time
import zipfile
from typing import Iterable, List, Optional, Tuple

import pandas as pd
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------------
# Configuration / Constants
# ----------------------------
CH_BASE = "https://download.companieshouse.gov.uk"
PRIMARY_FMT = "{base}/Accounts_Monthly_Data-{month}{year}.zip"
ARCHIVE_FMT = "{base}/archive/Accounts_Monthly_Data-{month}{year}.zip"

# Chunk size for streaming downloads
CHUNK = 1024 * 256

# Transient ODBC error codes we retry
TRANSIENT_ODBC_STATE = {"08S01", "40001", "HYT00", "HYT01"}

# ----------------------------
# Utilities
# ----------------------------
def month_str(dt_month: dt.date) -> str:
    return dt_month.strftime("%Y-%m")


def iter_months(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = dt.date(start.year, start.month, 1)
    last = dt.date(end.year, end.month, 1)
    while d <= last:
        yield d
        # add one month
        year = d.year + (d.month // 12)
        month = (d.month % 12) + 1
        d = dt.date(year, month, 1)


def month_to_urls(d: dt.date) -> List[str]:
    # e.g., January2025
    month_name = d.strftime("%B")
    year = d.strftime("%Y")
    mtoken = f"{month_name}{year}"
    return [
        PRIMARY_FMT.format(base=CH_BASE, month=month_name, year=year),
        ARCHIVE_FMT.format(base=CH_BASE, month=month_name, year=year),
        # Some mirrors have no camel case month — keep just in case:
        PRIMARY_FMT.format(base=CH_BASE, month=month_name, year=year).replace(
            "Monthly_Data-", "Monthly Data-"
        ),
        ARCHIVE_FMT.format(base=CH_BASE, month=month_name, year=year).replace(
            "Monthly_Data-", "Monthly Data-"
        ),
    ]


def build_http_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.25,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(
        {
            "User-Agent": "ixbrl-financials-db/1.0 (+https://github.com/)",
            "Accept": "*/*",
            "Connection": "keep-alive",
        }
    )
    return s


def _download_with_validation(session: requests.Session, url: str, max_attempts: int = 5) -> Optional[bytes]:
    for attempt in range(1, max_attempts + 1):
        try:
            r = session.get(url, stream=True, timeout=(10, 120))
            status = r.status_code
            if status == 404:
                return None  # definite miss; try next URL
            if status >= 400:
                # let Retry in adapter handle next try
                raise requests.HTTPError(f"{status} for {url}")

            expected = r.headers.get("Content-Length")
            expected_size = int(expected) if expected and expected.isdigit() else None

            buf = io.BytesIO()
            for chunk in r.iter_content(chunk_size=CHUNK):
                if not chunk:
                    continue
                buf.write(chunk)

            data = buf.getvalue()
            if expected_size is not None and len(data) != expected_size:
                raise IOError(f"incomplete body: got {len(data)} expected {expected_size}")

            # quick sanity: is ZIP?
            if not zipfile.is_zipfile(io.BytesIO(data)):
                raise IOError("downloaded file is not a valid ZIP")

            return data

        except Exception as e:
            print(f"    fetch attempt {attempt}/{max_attempts} failed: {e!s}")
            time.sleep(min(2 * attempt, 10))
    return None


def fetch_month_zip(session: requests.Session, d: dt.date) -> Optional[bytes]:
    print(f"Fetching month: {month_str(d)} -> ", end="", flush=True)
    for url in month_to_urls(d):
        print(url)
        data = _download_with_validation(session, url)
        if data is None:
            # 404 or ultimate failure on this URL; try next
            continue
        return data
    print(f"{month_str(d)}: no available monthly ZIP (skipping).")
    return None


def sanitize_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return name


def qident(schema: str, table: str) -> str:
    return f"[{sanitize_ident(schema)}].[{sanitize_ident(table)}]"


def connect_odbc(odbc_connect: str, attempts: int = 5, sleep: int = 5) -> pyodbc.Connection:
    last = None
    for i in range(1, attempts + 1):
        try:
            cn = pyodbc.connect(odbc_connect, timeout=30, autocommit=False)
            return cn
        except pyodbc.Error as e:
            last = e
            print(f"[attempt {i}/{attempts}] connect failed: {e}")
            time.sleep(sleep)
    if last:
        raise last
    raise RuntimeError("Could not connect")


def create_sqlalchemy_engine(odbc_connect: str) -> Engine:
    # Note: odbc_connect is a full ODBC connection string
    uri = f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(odbc_connect)}"
    engine = create_engine(
        uri,
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=180,
    )
    return engine


def ensure_staging_table(engine: Engine, schema: str, staging_table: str, columns: List[str]) -> None:
    stg = qident(schema, staging_table)
    col_defs = ",\n".join([f"[{c}] NVARCHAR(MAX) NULL" for c in columns])
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :table
)
BEGIN
    EXEC('CREATE TABLE {stg} (
{col_defs}
    )');
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql), {"schema": sanitize_ident(schema), "table": sanitize_ident(staging_table)})


def clear_staging(engine: Engine, schema: str, staging_table: str) -> None:
    stg = qident(schema, staging_table)
    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('{stg}', 'U') IS NOT NULL TRUNCATE TABLE {stg};"))


def rows_from_zip(data: bytes) -> pd.DataFrame:
    # The monthly ZIP contains one CSV (occasionally multiple). We read all CSVs and concat.
    dfs: List[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            with zf.open(name) as fh:
                # The monthly files are large; use low_mem=False to keep types sane and avoid dtype warnings.
                df = pd.read_csv(fh, low_memory=False)
                dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)

    # Normalize expected columns set (rename to our canonical names where needed)
    # Keep a stable, generous superset to avoid schema drift breaking the run.
    rename_map = {
        "Companies House Registered Number": "companies_house_registered_number",
        "Date": "date",
        "Balance Sheet Date": "balance_sheet_date",
        "Company Name": "entity_current_legal_name",
        "Dormant Company": "company_dormant",
        "Average Number Employees During Period": "average_number_employees_during_period",
        "Period Start": "period_start",
        "Period End": "period_end",
        "Tangible Fixed Assets": "tangible_fixed_assets",
        "Debtors": "debtors",
        "Cash Bank On Hand": "cash_bank_in_hand",
        "Current Assets": "current_assets",
        "Creditors Due Within One Year": "creditors_due_within_one_year",
        "Creditors Due After One Year": "creditors_due_after_one_year",
        "Net Current Assets Liabilities": "net_current_assets_liabilities",
        "Total Assets Less Current Liabilities": "total_assets_less_current_liabilities",
        "Net Assets Liabilities Including Pension Asset Liability": "net_assets_liabilities_including_pension_asset_liability",
        "Called Up Share Capital": "called_up_share_capital",
        "Profit Loss Account Reserve": "profit_loss_account_reserve",
        "Shareholder Funds": "shareholder_funds",
        "Turnover Gross Operating Revenue": "turnover_gross_operating_revenue",
        "Other Operating Income": "other_operating_income",
        "Cost Sales": "cost_sales",
        "Gross Profit Loss": "gross_profit_loss",
        "Administrative Expenses": "administrative_expenses",
        "Raw Materials Consumables": "raw_materials_consumables",
        "Staff Costs": "staff_costs",
        "Depreciation Other Amounts Written Off Tangible Intangible Fixed Assets": "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
        "Other Operating Charges Format2": "other_operating_charges_format2",
        "Operating Profit Loss": "operating_profit_loss",
        "Profit Loss On Ordinary Activities Before Tax": "profit_loss_on_ordinary_activities_before_tax",
        "Tax On Profit Or Loss On Ordinary Activities": "tax_on_profit_or_loss_on_ordinary_activities",
        "Profit Loss For Period": "profit_loss_for_period",
        "File Type": "file_type",
        "Taxonomy": "taxonomy",
        "Company ID": "company_id",  # if present
        "Run Code": "run_code",
    }
    df = df.rename(columns=rename_map)

    # Ensure required keys exist
    for req in ("companies_house_registered_number", "date"):
        if req not in df.columns:
            df[req] = None

    # Date-like columns to datetime (best effort)
    date_cols = [
        "date",
        "balance_sheet_date",
        "period_start",
        "period_end",
    ]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # force types that help MERGE keys
    if "companies_house_registered_number" in df.columns:
        df["companies_house_registered_number"] = df["companies_house_registered_number"].astype(str).str.zfill(8)

    # Provide company_id if missing (mirror CHRN)
    if "company_id" not in df.columns:
        df["company_id"] = df["companies_house_registered_number"]

    return df


def write_stage(engine: Engine, schema: str, staging_table: str, df: pd.DataFrame) -> None:
    stg = qident(schema, staging_table)
    # All columns as NVARCHAR(MAX) in staging; convert to string where needed.
    df2 = df.copy()
    # make strings for all non-datetime columns
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            continue
        df2[c] = df2[c].astype(object).where(df2[c].notna(), None)

    # to_sql (replace=False) into existing staging
    with engine.begin() as conn:
        # Use fast_executemany underneath via engine setting
        df2.to_sql(staging_table, con=conn, schema=schema, if_exists="append", index=False, method="multi")


def merge_stage_into_target(engine: Engine, schema: str, staging_table: str, target_table: str) -> None:
    stg = qident(schema, staging_table)
    tgt = qident(schema, target_table)

    # NOTE: Adjust the column list below to match your real dbo.financials schema.
    # This is a safe superset matching what we normalized in rows_from_zip().
    cols = [
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
        "zip_url",
        "error",
    ]

    select_cols = ", ".join(f"S.[{c}]" for c in cols if c)  # skip missing safely in SQL

    # Build column list that actually exists in target
    with engine.begin() as conn:
        # fetch target columns
        q = text(
            """
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON t.object_id = c.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = :schema AND t.name = :table
ORDER BY c.column_id
"""
        )
        rows = conn.execute(q, {"schema": sanitize_ident(schema), "table": sanitize_ident(target_table)}).fetchall()
        target_cols = {r[0] for r in rows}

    insert_cols = [c for c in cols if c in target_cols]
    insert_cols_sql = ", ".join(f"[{c}]" for c in insert_cols)
    values_cols_sql = ", ".join(f"S.[{c}]" for c in insert_cols)

    # Upsert on (company_id, date) — adjust key if your PK differs
    merge_sql = f"""
MERGE {tgt} AS T
USING (SELECT * FROM {stg}) AS S
    ON (T.[company_id] = S.[company_id] AND T.[date] = S.[date])
WHEN MATCHED THEN
    UPDATE SET
        -- update selective columns; keep keys intact
        T.[file_type] = S.[file_type],
        T.[taxonomy] = S.[taxonomy],
        T.[balance_sheet_date] = S.[balance_sheet_date],
        T.[entity_current_legal_name] = S.[entity_current_legal_name],
        T.[company_dormant] = S.[company_dormant],
        T.[average_number_employees_during_period] = S.[average_number_employees_during_period],
        T.[period_start] = S.[period_start],
        T.[period_end] = S.[period_end],
        T.[tangible_fixed_assets] = S.[tangible_fixed_assets],
        T.[debtors] = S.[debtors],
        T.[cash_bank_in_hand] = S.[cash_bank_in_hand],
        T.[current_assets] = S.[current_assets],
        T.[creditors_due_within_one_year] = S.[creditors_due_within_one_year],
        T.[creditors_due_after_one_year] = S.[creditors_due_after_one_year],
        T.[net_current_assets_liabilities] = S.[net_current_assets_liabilities],
        T.[total_assets_less_current_liabilities] = S.[total_assets_less_current_liabilities],
        T.[net_assets_liabilities_including_pension_asset_liability] = S.[net_assets_liabilities_including_pension_asset_liability],
        T.[called_up_share_capital] = S.[called_up_share_capital],
        T.[profit_loss_account_reserve] = S.[profit_loss_account_reserve],
        T.[shareholder_funds] = S.[shareholder_funds],
        T.[turnover_gross_operating_revenue] = S.[turnover_gross_operating_revenue],
        T.[other_operating_income] = S.[other_operating_income],
        T.[cost_sales] = S.[cost_sales],
        T.[gross_profit_loss] = S.[gross_profit_loss],
        T.[administrative_expenses] = S.[administrative_expenses],
        T.[raw_materials_consumables] = S.[raw_materials_consumables],
        T.[staff_costs] = S.[staff_costs],
        T.[depreciation_other_amounts_written_off_tangible_intangible_fixed_assets] = S.[depreciation_other_amounts_written_off_tangible_intangible_fixed_assets],
        T.[other_operating_charges_format2] = S.[other_operating_charges_format2],
        T.[operating_profit_loss] = S.[operating_profit_loss],
        T.[profit_loss_on_ordinary_activities_before_tax] = S.[profit_loss_on_ordinary_activities_before_tax],
        T.[tax_on_profit_or_loss_on_ordinary_activities] = S.[tax_on_profit_or_loss_on_ordinary_activities],
        T.[profit_loss_for_period] = S.[profit_loss_for_period],
        T.[zip_url] = S.[zip_url],
        T.[error] = S.[error]
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols})
    VALUES ({values_cols_sql});
"""
    with engine.begin() as conn:
        conn.execute(text(merge_sql))


def robust_stage_and_merge(engine: Engine, schema: str, staging_table: str, target_table: str, df: pd.DataFrame) -> None:
    # Prepare staging table (create if missing)
    ensure_staging_table(engine, schema, staging_table, list(df.columns))
    # Clean prior staging (safe even if empty)
    clear_staging(engine, schema, staging_table)

    # Stage rows
    try:
        write_stage(engine, schema, staging_table, df)
    except Exception as e:
        # Make sure no bad transaction lingers
        with engine.connect() as conn:
            try:
                conn.exec_driver_sql("ROLLBACK TRAN")
            except Exception:
                pass
        raise

    # Merge into target
    merge_stage_into_target(engine, schema, staging_table, target_table)
    # Clear staging again (optional)
    clear_staging(engine, schema, staging_table)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--months", nargs="+", help="Explicit months like 2025-01 2025-02")
    g.add_argument("--start-month", help="Inclusive start month YYYY-MM")
    p.add_argument("--end-month", help="Inclusive end month YYYY-MM (required with --start-month)")
    g.add_argument("--since-months", type=int, help="Ingest from N months ago up to current month")

    p.add_argument("--odbc-connect", required=True, help="ODBC connection string for SQL Server")
    p.add_argument("--schema", required=True, help="Target schema, e.g., dbo")
    p.add_argument("--target-table", required=True, help="Target table, e.g., financials")
    p.add_argument("--staging-table", default="financials_stage", help="Temporary staging table name")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


def months_from_args(args: argparse.Namespace) -> List[dt.date]:
    if args.months:
        return [dt.datetime.strptime(m, "%Y-%m").date().replace(day=1) for m in args.months]
    if args.start_month:
        if not args.end_month:
            raise SystemExit("--end-month is required with --start-month")
        s = dt.datetime.strptime(args.start_month, "%Y-%m").date().replace(day=1)
        e = dt.datetime.strptime(args.end_month, "%Y-%m").date().replace(day=1)
        return list(iter_months(s, e))
    # since-months
    today = dt.date.today().replace(day=1)
    s = (today - pd.DateOffset(months=args.since_months)).date().replace(day=1)
    return list(iter_months(s, today))


def main() -> int:
    args = parse_args()
    months = months_from_args(args)
    print(f"*** --schema {args.schema} --target-table {args.target_table}")
    print(f"Target months: {[m.strftime('%Y-%m') for m in months]}")

    # DB engine
    engine = create_sqlalchemy_engine(args.odbc_connect)

    session = build_http_session()

    total_ingested = 0

    for m in months:
        urls = month_to_urls(m)
        print(f"Fetching month: {m.strftime('%Y-%m')} -> {urls[0]}")
        try:
            blob = fetch_month_zip(session, m)
            if not blob:
                continue

            df = rows_from_zip(blob)
            if df.empty:
                print(f"{m.strftime('%Y-%m')}: ZIP had no CSV rows, skipping.")
                continue

            # annotate run info
            df["zip_url"] = urls[0]
            df["error"] = ""

            # Stage + MERGE with robust transaction/rollback
            try:
                robust_stage_and_merge(engine, args.schema, args.staging_table, args.target_table, df)
                total_ingested += len(df)
                print(f"{m.strftime('%Y-%m')}: staged/merged {len(df)} rows.")
            except pyodbc.Error as e:
                # If we hit a transient error, try once more
                state = getattr(e, "args", [None])[0] or ""
                if any(s in str(e) for s in TRANSIENT_ODBC_STATE) or state in TRANSIENT_ODBC_STATE:
                    print(f"{m.strftime('%Y-%m')}: transient SQL error, retrying once... ({e})")
                    time.sleep(5)
                    robust_stage_and_merge(engine, args.schema, args.staging_table, args.target_table, df)
                    total_ingested += len(df)
                    print(f"{m.strftime('%Y-%m')}: staged/merged after retry ({len(df)} rows).")
                else:
                    raise

        except requests.HTTPError as e:
            print(f"{m.strftime('%Y-%m')}: fetch/parse error: {e}")
        except Exception as e:
            print(f"{m.strftime('%Y-%m')}: staging/merge error: {e}")
            return 1

    if total_ingested == 0:
        print("Nothing ingested (no files found or all months empty).")
    else:
        print(f"Done. Total rows staged/merged: {total_ingested}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
