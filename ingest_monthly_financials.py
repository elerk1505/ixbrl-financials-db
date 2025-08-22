#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monthly iXBRL -> Azure SQL (staging + upsert)
- Range   : --start-month YYYY-MM --end-month YYYY-MM
- List    : --months 2025-01,2025-02
- Lookback: --since-months N  (N=1 => current month only)

Requires: httpx pandas sqlalchemy pyodbc stream-read-xbrl
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

# ---- expected target columns (order doesn’t matter) ----
TARGET_COLUMNS: Tuple[str, ...] = (
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
    "error","zip_url"
)

# ---- engine ----
def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    return create_engine(engine_url, fast_executemany=True, future=True)

# ---- month helpers ----
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

# ---- CH URLs ----
def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # January, ...

def candidate_urls_for_month(ym: str) -> List[str]:
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls

# ---- download/parse ----
def fetch_month_zip_as_rows(yyyy_mm: str, timeout: float = 180.0, max_attempts: int = 5):
    """
    Streams the monthly zip and yields (columns, rows) by delegating to stream_read_xbrl.
    Retries on network hiccups and incomplete bodies. Returns (columns, rows).
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        for attempt in range(1, max_attempts + 1):
            try:
                with httpx.stream("GET", url, timeout=timeout) as r:
                    r.raise_for_status()
                    expected = int(r.headers.get("Content-Length", "0") or "0")
                    total = 0
                    def _iter():
                        nonlocal total
                        for chunk in r.iter_bytes():
                            total += len(chunk)
                            yield chunk
                    with stream_read_xbrl_zip(_iter()) as (columns, rows):
                        # If server didn't set Content-Length we can't verify; otherwise check.
                        if expected and total != expected:
                            raise IOError(f"incomplete body: got {total} expected {expected}")
                        return columns, rows, url
            except httpx.HTTPStatusError as e:
                # 404 => try next candidate URL
                if e.response is not None and e.response.status_code == 404:
                    print(f"  {yyyy_mm}: 404 at {url} (attempt {attempt}/{max_attempts}), trying next URL…")
                    last_err = e
                    break
                last_err = e
            except Exception as e:
                last_err = e
                print(f"  {yyyy_mm}: fetch attempt {attempt}/{max_attempts} failed: {e}")
                time.sleep(min(2 * attempt, 10))
        # next candidate URL
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No usable URL for {yyyy_mm}")

# ---- cleaning / shaping ----
def to_dataframe(columns: List[str], rows_iter, zip_url: str) -> pd.DataFrame:
    # Convert all None to '' then to string for uniformity.
    data = [[("" if v is None else v) for v in row] for row in rows_iter]
    df = pd.DataFrame(data, columns=columns)

    # Drop duplicated column headers (keep first)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    # Company number normalization
    cname = "companies_house_registered_number"
    for alt in ("company_number",):
        if alt in df.columns and cname not in df.columns:
            df = df.rename(columns={alt: cname})
    if cname in df.columns:
        df[cname] = df[cname].astype(str).str.replace(" ", "", regex=False)

    # Harmonize dates -> text ISO for staging
    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            # Keep old behavior of returning ndarray to avoid future warning noise
            df[c] = np.array(s.dt.to_pydatetime(), dtype=object)

    # Keep only columns we care about
    keep = [c for c in TARGET_COLUMNS if c in df.columns]  # from source
    # add missing expected cols
    for c in TARGET_COLUMNS:
        if c not in keep:
            df[c] = None
            keep.append(c)
    df = df[keep].copy()

    # Coerce empty strings to None for numeric-ish columns (SQL Server likes NULL not '')
    numericish = {
        "average_number_employees_during_period","tangible_fixed_assets","debtors","cash_bank_in_hand",
        "current_assets","creditors_due_within_one_year","creditors_due_after_one_year",
        "net_current_assets_liabilities","total_assets_less_current_liabilities",
        "net_assets_liabilities_including_pension_asset_liability","called_up_share_capital",
        "profit_loss_account_reserve","shareholder_funds","turnover_gross_operating_revenue",
        "other_operating_income","cost_sales","gross_profit_loss","administrative_expenses",
        "raw_materials_consumables","staff_costs",
        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
        "other_operating_charges_format2","operating_profit_loss",
        "profit_loss_on_ordinary_activities_before_tax",
        "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period"
    }
    for c in (numericish & set(df.columns)):
        df[c] = df[c].replace("", np.nan)

    # company_dormant as 0/1 if it looks like bool text
    if "company_dormant" in df.columns:
        df["company_dormant"] = (
            df["company_dormant"]
            .astype(str)
            .str.lower()
            .map({"true": 1, "false": 0, "1": 1, "0": 0})
            .where(lambda s: s.notna(), None)
        )

    # Always fill run_code + zip_url where missing
    if "run_code" in df.columns and df["run_code"].isna().all():
        df["run_code"] = f"Prod{datetime.utcnow():%y%m}_{len(df)}"
    if "zip_url" in df.columns:
        df["zip_url"] = zip_url

    return df

# ---- SQL helpers ----
def ensure_staging_table(engine: Engine, schema: str, stage: str):
    # All columns NVARCHAR(MAX) for staging, and [error] is quoted to avoid keyword issues
    ddl_cols = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in TARGET_COLUMNS)
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :table
)
BEGIN
    EXEC('CREATE TABLE [{schema}].[{stage}] (
        {ddl_cols}
    )');
END
""".replace("{schema}", schema).replace("{stage}", stage)
    with engine.begin() as conn:
        conn.execute(text(sql), {"schema": schema, "table": stage})

def write_stage(engine: Engine, schema: str, stage: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    # Convert datetimes to ISO strings (SQL NVARCHAR staging)
    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce")
            df[c] = s.dt.strftime("%Y-%m-%d")

    df = df.astype(object).where(pd.notna(df), None)

    # executemany into staging
    cols = list(df.columns)
    qs = ",".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{schema}].[{stage}] ({','.join('['+c+']' for c in cols)}) VALUES ({qs})"

    rows = [tuple(df.iloc[i].tolist()) for i in range(len(df))]
    with engine.begin() as conn:
        cur = conn.connection.cursor()  # raw DBAPI for speed
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)
    return len(df)

def upsert_into_final(engine: Engine, schema: str, stage: str, target: str) -> int:
    """
    Merge by primary key (companies_house_registered_number, date).
    Update non-key cols; insert when not exists.
    """
    key_a = "companies_house_registered_number"
    key_b = "date"
    set_cols = [c for c in TARGET_COLUMNS if c not in (key_a, key_b)]
    set_clause = ",\n        ".join(f"T.[{c}] = S.[{c}]" for c in set_cols)
    insert_cols = ", ".join(f"[{c}]" for c in TARGET_COLUMNS)
    insert_vals = ", ".join(f"S.[{c}]" for c in TARGET_COLUMNS)

    merge_sql = f"""
MERGE [{schema}].[{target}] AS T
USING (SELECT * FROM [{schema}].[{stage}]) AS S
  ON T.[{key_a}] = S.[{key_a}] AND T.[{key_b}] = S.[{key_b}]
WHEN MATCHED THEN UPDATE SET
        {set_clause}
WHEN NOT MATCHED THEN
  INSERT ({insert_cols})
  VALUES ({insert_vals});
"""
    with engine.begin() as conn:
        res = conn.exec_driver_sql(merge_sql)
        # rowcount is unreliable for MERGE; we’ll just return 0 and rely on logs.
    return 0

def clear_stage(engine: Engine, schema: str, stage: str):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE [{schema}].[{stage}]")

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]

# ---- main ----
def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months")
    ap.add_argument("--since-months", type=int, default=1)
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--download-retries", type=int, default=5)
    args = ap.parse_args()

    # Build months
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    print(f"Target months: {months}")

    # Engine
    engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)

    # Ensure staging exists
    ensure_staging_table(engine, args.schema, args.staging_table)

    # Sanity: target table exists & columns superset (we won’t re-create it here)
    try:
        target_cols = set(get_table_columns(engine, args.schema, args.target_table))
        missing = set(TARGET_COLUMNS) - target_cols
        if missing:
            print(f"WARNING: Target table {args.schema}.{args.target_table} is missing columns: {sorted(missing)}")
    except Exception as e:
        print(f"ERROR inspecting target table: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # fetch
        try:
            cols, rows, url = fetch_month_zip_as_rows(ym, timeout=args.timeout, max_attempts=args.download_retries)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        # shape
        df = to_dataframe(cols, rows, zip_url=url)
        if df.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # stage -> merge -> clear
        try:
            clear_stage(engine, args.schema, args.staging_table)
            staged = write_stage(engine, args.schema, args.staging_table, df)
            print(f"{ym}: staged {staged:,} rows.")
            upsert_into_final(engine, args.schema, args.staging_table, args.target_table)
            print(f"✅ {ym}: upsert complete into {args.schema}.{args.target_table}.")
            wrote_any = True
        except SQLAlchemyError as e:
            print(f"{ym}: SQL error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: unexpected SQL error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
