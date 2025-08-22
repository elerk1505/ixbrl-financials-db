#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (robust, idempotent)

Usage examples:
  --start-month 2025-01 --end-month 2025-06
  --months 2024-11,2024-12
  --since-months 1           # current month only (default)

Environment (optional):
  AZURE_SQL_URL   (SQLAlchemy URL) OR
  AZURE_SQL_ODBC  (pyodbc ODBC connect string)

Requires:
  httpx pandas sqlalchemy pyodbc stream-read-xbrl

Table contract:
  Target table (default dbo.financials) has these 39 columns (NVARCHAR/DATE ok):
    run_code, company_id, date, file_type, taxonomy, balance_sheet_date,
    companies_house_registered_number, entity_current_legal_name, company_dormant,
    average_number_employees_during_period, period_start, period_end,
    tangible_fixed_assets, debtors, cash_bank_in_hand, current_assets,
    creditors_due_within_one_year, creditors_due_after_one_year,
    net_current_assets_liabilities, total_assets_less_current_liabilities,
    net_assets_liabilities_including_pension_asset_liability, called_up_share_capital,
    profit_loss_account_reserve, shareholder_funds, turnover_gross_operating_revenue,
    other_operating_income, cost_sales, gross_profit_loss, administrative_expenses,
    raw_materials_consumables, staff_costs,
    depreciation_other_amounts_written_off_tangible_intangible_fixed_assets,
    other_operating_charges_format2, operating_profit_loss,
    profit_loss_on_ordinary_activities_before_tax,
    tax_on_profit_or_loss_on_ordinary_activities, profit_loss_for_period,
    error, zip_url

Primary key (assumed for upsert):
  (companies_house_registered_number, period_end)
"""

from __future__ import annotations
import argparse
import os
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# Column contract (39)
# -----------------------------
COLUMNS: List[str] = [
    "run_code","company_id","date","file_type","taxonomy","balance_sheet_date",
    "companies_house_registered_number","entity_current_legal_name","company_dormant",
    "average_number_employees_during_period","period_start","period_end",
    "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
    "creditors_due_within_one_year","creditors_due_after_one_year",
    "net_current_assets_liabilities","total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability","called_up_share_capital",
    "profit_loss_account_reserve","shareholder_funds","turnover_gross_operating_revenue",
    "other_operating_income","cost_sales","gross_profit_loss","administrative_expenses",
    "raw_materials_consumables","staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2","operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
    "error","zip_url",
]

DATE_COLS = {"date","balance_sheet_date","period_start","period_end"}

# -----------------------------
# Utilities
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
# HTTP fetch with validation
# -----------------------------
def month_name_en(ym: str) -> str:
    import calendar
    return calendar.month_name[parse_yyyy_mm(ym).month]

def candidate_urls_for_month(ym: str) -> List[str]:
    """Newest to oldest locations. DO NOT change ordering."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    urls = [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip",
    ]
    if y in (2008, 2009):
        urls.append(
            f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip"
        )
    return urls

class IncompleteBody(Exception): pass

def _fetch_bytes_with_retries(url: str, timeout: float, attempts: int = 5) -> bytes:
    last_exc: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout, headers={"Accept": "*/*"}) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", "0") or "0")
                chunks: List[bytes] = []
                total = 0
                for chunk in r.iter_bytes():
                    if chunk:
                        chunks.append(chunk)
                        total += len(chunk)
                data = b"".join(chunks)
                if expected and total != expected:
                    raise IncompleteBody(f"incomplete body: got {total} expected {expected}")
                return data
        except Exception as e:
            last_exc = e
            print(f"    fetch attempt {i}/{attempts} failed: {e}")
            time.sleep(min(2*i, 10))
    assert last_exc is not None
    raise last_exc

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> Tuple[pd.DataFrame, str]:
    """Return (DataFrame, url_used). Raises if all candidates fail."""
    from stream_read_xbrl import stream_read_xbrl_zip
    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            blob = _fetch_bytes_with_retries(url, timeout=timeout, attempts=5)
            with stream_read_xbrl_zip(iter([blob])) as (columns, rows):
                data = [[("" if v is None else str(v)) for v in row] for row in rows]
            df = pd.DataFrame(data, columns=columns)
            return dedupe_columns(df), url
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"    404 at {url}, trying next URL…")
                last_err = e
                continue
            last_err = e
            break
        except Exception as e:
            last_err = e
            print(f"    parse/fetch error on {url}: {e}")
            # try next candidate
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# -----------------------------
# Normalisation -> target contract
# -----------------------------
def normalise_df(df: pd.DataFrame, zip_url: str) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Canonical company number
    if "companies_house_registered_number" in df.columns:
        s = series_or_first(df, "companies_house_registered_number")
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Preferred period_end mapping
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Ensure all 39 columns exist; fill missing with None
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None

    # Keep only the contract columns & order
    df = df[COLUMNS].copy()

    # Coerce date columns
    for c in DATE_COLS:
        s = series_or_first(df, c)
        df[c] = pd.to_datetime(s, errors="coerce").dt.date

    # Attach the source zip_url (overwriting if present)
    df["zip_url"] = zip_url

    # A few common numeric-like columns sometimes come as DFs; force to string
    return df

# -----------------------------
# SQL helpers (robust)
# -----------------------------
def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")

    if not engine_url:
        import urllib.parse
        # Harden the ODBC connect string
        if "Encrypt=" not in odbc_connect:
            odbc_connect += ";Encrypt=yes"
        if "TrustServerCertificate=" not in odbc_connect:
            odbc_connect += ";TrustServerCertificate=no"
        if "Connection Timeout=" not in odbc_connect:
            odbc_connect += ";Connection Timeout=60"
        if "LoginTimeout=" not in odbc_connect:
            odbc_connect += ";LoginTimeout=30"
        if "ConnectRetryCount=" not in odbc_connect:
            odbc_connect += ";ConnectRetryCount=3;ConnectRetryInterval=10"
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    # fast_executemany True helps bulk to staging
    return create_engine(engine_url, fast_executemany=True, future=True)

def ensure_staging_table(engine: Engine, schema: str, stage_table: str) -> None:
    # Create if missing (NVARCHAR(MAX) for text; DATE for date columns)
    cols_sql = []
    for c in COLUMNS:
        if c in DATE_COLS:
            cols_sql.append(f"[{c}] DATE NULL")
        else:
            cols_sql.append(f"[{c}] NVARCHAR(MAX) NULL")
    cols_sql_str = ",\n".join(cols_sql)
    full_name = f"[{schema}].[{stage_table}]"
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = '{schema}' AND t.name = '{stage_table}'
)
BEGIN
    EXEC('CREATE TABLE {full_name} (
{cols_sql_str}
    )');
END
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def truncate_staging(engine: Engine, schema: str, stage_table: str) -> None:
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql(f"TRUNCATE TABLE [{schema}].[{stage_table}]")
        except SQLAlchemyError:
            conn.exec_driver_sql(f"DELETE FROM [{schema}].[{stage_table}]")

def to_sql_stage(df: pd.DataFrame, engine: Engine, schema: str, stage_table: str) -> int:
    if df.empty:
        return 0
    df.to_sql(name=stage_table, con=engine, schema=schema, if_exists="append", index=False,
              chunksize=1000, method=None)
    return len(df)

def merge_stage_into_final(engine: Engine, schema: str, stage_table: str, final_table: str) -> int:
    tgt = f"[{schema}].[{final_table}]"
    src = f"[{schema}].[{stage_table}]"

    # Build dynamic MERGE SET for all non-key columns
    key1, key2 = "companies_house_registered_number", "period_end"
    non_keys = [c for c in COLUMNS if c not in (key1, key2)]
    set_list = ", ".join([f"t.[{c}] = s.[{c}]" for c in non_keys])
    cols_list = ", ".join([f"[{c}]" for c in COLUMNS])
    vals_list = ", ".join([f"s.[{c}]" for c in COLUMNS])

    merge_sql = f"""
MERGE {tgt} AS t
USING (SELECT {cols_list} FROM {src}) AS s
ON (t.[{key1}] = s.[{key1}] AND t.[{key2}] = s.[{key2}])
WHEN MATCHED THEN UPDATE SET {set_list}
WHEN NOT MATCHED THEN INSERT ({cols_list}) VALUES ({vals_list});
SELECT @@ROWCOUNT AS merged_rows;
"""
    with engine.begin() as conn:
        res = conn.exec_driver_sql(merge_sql).scalar()
        return int(res or 0)

# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (robust).")
    # Range / list / lookback
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
    # Net / retries
    ap.add_argument("--timeout", type=float, default=180.0)

    args = ap.parse_args()

    # Build month list
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

    # Ensure staging
    try:
        ensure_staging_table(engine, args.schema, args.staging_table)
    except Exception as e:
        print(f"ERROR creating/checking staging: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # Download + parse
        try:
            df_raw, used_url = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        if df_raw.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        df = normalise_df(df_raw, used_url)

        # Stage (truncate before each month to keep MERGE batches small)
        try:
            truncate_staging(engine, args.schema, args.staging_table)
        except Exception as e:
            print(f"{ym}: staging cleanup error: {e}")
            return 1

        # Write to staging with basic retry (handles transient link failures)
        last_err: Optional[Exception] = None
        for attempt in range(1, 6):
            try:
                rows = to_sql_stage(df, engine, args.schema, args.staging_table)
                break
            except SQLAlchemyError as e:
                last_err = e
                print(f"{ym}: staging SQL error: {e}")
                time.sleep(3 * attempt)
            except Exception as e:
                last_err = e
                print(f"{ym}: staging unexpected error: {e}")
                time.sleep(3 * attempt)
        if last_err:
            print("Giving up due to staging error.")
            return 1

        # Merge into final with retry (handles [08S01] SQLEndTran)
        for attempt in range(1, 6):
            try:
                merged = merge_stage_into_final(engine, args.schema, args.staging_table, args.target_table)
                print(f"{ym}: upserted {merged} rows into {args.schema}.{args.target_table}.")
                wrote_any = True
                break
            except SQLAlchemyError as e:
                print(f"{ym}: merge error (attempt {attempt}/5): {e}")
                time.sleep(4 * attempt)
            except Exception as e:
                print(f"{ym}: merge unexpected error (attempt {attempt}/5): {e}")
                time.sleep(4 * attempt)
        else:
            print(f"{ym}: MERGE failed after retries.")
            return 1

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
