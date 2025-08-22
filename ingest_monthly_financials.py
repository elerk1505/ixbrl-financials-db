#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (staging + MERGE upsert)

- Months via:
    --start-month YYYY-MM --end-month YYYY-MM     (inclusive range)
    --months 2024-01,2024-02                      (explicit list)
    --since-months N                              (lookback, default=1)

- Reads the official monthly iXBRL zips from Companies House.
- Normalises columns to your target schema and coerces types to avoid cast errors.
- Loads into a per-run staging table, then MERGEs into the final table, so
  duplicates (PK) are updated not inserted.
- Retries transient SQL connectivity errors (08S01 etc).
- Low-noise logging (no huge parameter dumps).

Requires: httpx, pandas, sqlalchemy, pyodbc, stream-read-xbrl
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

# ---------------------------------------------------------------------
# Target schema (exact set/order doesn’t matter – we align by name)
# ---------------------------------------------------------------------

TARGET_COLUMNS: List[str] = [
    "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date",
    "companies_house_registered_number", "entity_current_legal_name", "company_dormant",
    "average_number_employees_during_period", "period_start", "period_end",
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year",
    "net_current_assets_liabilities", "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
    "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
    "staff_costs", "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
    "error", "zip_url",
]

DATE_COLS = {"date", "balance_sheet_date", "period_start", "period_end"}
BOOL_COLS = {"company_dormant"}
TEXT_COLS = {
    "run_code", "company_id", "file_type", "taxonomy",
    "companies_house_registered_number", "entity_current_legal_name",
    "error", "zip_url",
}
NUM_COLS = set(TARGET_COLUMNS) - DATE_COLS - BOOL_COLS - TEXT_COLS

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    """
    Prefer ODBC string for Azure SQL; add a few resiliency knobs if missing.
    """
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")

    if not engine_url:
        import urllib.parse

        # Add connect retry hints if not present
        if "ConnectRetryCount" not in odbc_connect:
            odbc_connect += ";ConnectRetryCount=3"
        if "ConnectRetryInterval" not in odbc_connect:
            odbc_connect += ";ConnectRetryInterval=10"
        if "TrustServerCertificate" not in odbc_connect:
            # GH runners + Azure SQL often need this when using firewall IP rules
            odbc_connect += ";TrustServerCertificate=yes"

        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    # fast_executemany speeds parametrised bulk inserts via pyodbc
    return create_engine(engine_url, fast_executemany=True, future=True)

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def series_or_first(df: pd.DataFrame, name: str) -> pd.Series:
    obj = df[name]
    return obj.iloc[:, 0] if isinstance(obj, pd.DataFrame) else obj

def month_name_en(ym: str) -> str:
    import calendar
    y, m = [int(x) for x in ym.split("-")]
    return calendar.month_name[m]

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

def candidate_urls_for_month(ym: str) -> List[str]:
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    urls = [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip",
    ]
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
            return dedupe_columns(pd.DataFrame(data, columns=columns)).assign(zip_url=url)
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            break
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# ---------------------------------------------------------------------
# Normalisation → map fields and coerce types to avoid cast errors
# ---------------------------------------------------------------------

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Canonical company number
    if "companies_house_registered_number" in df.columns:
        s = series_or_first(df, "companies_house_registered_number")
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)
    elif "company_number" in df.columns:
        s = series_or_first(df, "company_number")
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Canonical 'period_end'
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Make sure the expected columns exist (fill missing with None)
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Coerce types:
    # Dates → pandas datetime (timezone-naive)
    for c in DATE_COLS:
        s = series_or_first(df, c)
        df[c] = pd.to_datetime(s, errors="coerce").dt.to_pydatetime()

    # Booleans → 0/1 (bit)
    for c in BOOL_COLS:
        s = series_or_first(df, c)
        if s.dtype == bool:
            df[c] = s.astype("int8")
        else:
            # strings like 'True'/'False'/'0'/'1' → 1/0, else NULL
            df[c] = s.astype(str).str.strip().str.lower().map({"true": 1, "1": 1, "false": 0, "0": 0})
            df.loc[~df[c].isin([0, 1]), c] = None

    # Numerics → float (NaN on bad) so they become NULL in SQL
    for c in NUM_COLS:
        s = series_or_first(df, c)
        df[c] = pd.to_numeric(s.replace({"": None, " ": None}), errors="coerce")

    # Text → stripped strings, empty → NULL
    for c in TEXT_COLS:
        s = series_or_first(df, c)
        df[c] = s.astype(object).where(pd.notnull(s), None)
        df[c] = df[c].apply(lambda x: (x.strip() if isinstance(x, str) else x) or None)

    # run_code: set a default if missing
    if df["run_code"].isna().all():
        df["run_code"] = f"Run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    return df[TARGET_COLUMNS].copy()

# ---------------------------------------------------------------------
# SQL staging + MERGE
# ---------------------------------------------------------------------

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]

def make_staging_name(base_table: str) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    return f"{base_table}_stg_{ts}"

def create_staging_table(engine: Engine, schema: str, staging: str) -> None:
    # Keep types simple & permissive to accept cleansed data
    # (dates as DATETIME2, numbers as FLOAT, text as NVARCHAR(MAX), bool as BIT)
    col_defs = []
    for c in TARGET_COLUMNS:
        if c in DATE_COLS:
            sqlt = "DATETIME2 NULL"
        elif c in BOOL_COLS:
            sqlt = "BIT NULL"
        elif c in NUM_COLS:
            sqlt = "FLOAT NULL"
        else:
            sqlt = "NVARCHAR(MAX) NULL"
        col_defs.append(f"[{c}] {sqlt}")
    ddl = f"CREATE TABLE [{schema}].[{staging}] (\n  " + ",\n  ".join(col_defs) + "\n)"
    with engine.begin() as conn:
        conn.execute(text(ddl))

def drop_table_if_exists(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"""
            IF OBJECT_ID(N'[{schema}].[{table}]', N'U') IS NOT NULL
            DROP TABLE [{schema}].[{table}];
        """))

def write_stage_with_retry(engine: Engine, schema: str, staging: str, df: pd.DataFrame,
                           max_retries: int = 5) -> int:
    # Use pandas.to_sql for bulk insert into staging
    attempt = 0
    while True:
        try:
            df.to_sql(staging, engine, schema=schema, if_exists="append", index=False, chunksize=1000, method=None)
            return len(df)
        except SQLAlchemyError as e:
            attempt += 1
            msg = str(e.orig) if hasattr(e, "orig") else str(e)
            print(f"[stage attempt {attempt}/{max_retries}] {msg}")
            if "08S01" in msg and attempt < max_retries:
                time.sleep(5)
                continue
            raise

def merge_into_target(engine: Engine, schema: str, staging: str, target: str) -> Tuple[int, int]:
    """
    Upsert using PK assumption: (companies_house_registered_number, date)
    - UPDATE on match
    - INSERT on not matched
    Returns (#updated, #inserted)
    """
    # Build dynamic SET for all non-PK columns
    pk = ["companies_house_registered_number", "date"]
    non_pk = [c for c in TARGET_COLUMNS if c not in pk]

    set_clause = ",\n    ".join([f"T.[{c}] = S.[{c}]" for c in non_pk])
    insert_cols = ", ".join([f"[{c}]" for c in TARGET_COLUMNS])
    insert_vals = ", ".join([f"S.[{c}]" for c in TARGET_COLUMNS])
    on_clause = " AND ".join([f"T.[{c}] = S.[{c}]" for c in pk])

    # Use OUTPUT to get counts
    sql = f"""
    DECLARE @merge_results TABLE (action NVARCHAR(10));

    MERGE [{schema}].[{target}] AS T
    USING (SELECT {insert_cols} FROM [{schema}].[{staging}]) AS S ({insert_cols})
      ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET
        {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols}) VALUES ({insert_vals})
    OUTPUT $action INTO @merge_results;

    SELECT
      SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END) AS updated,
      SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END) AS inserted
    FROM @merge_results;
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql)).mappings().one()
        return int(res["updated"] or 0), int(res["inserted"] or 0)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (staging + MERGE).")
    # Months
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months")
    ap.add_argument("--since-months", type=int, default=1)
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--timeout", type=float, default=180.0)

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

    # Check target columns (informative)
    try:
        targ_cols = set(get_table_columns(engine, args.schema, args.target_table))
        missing_in_target = [c for c in TARGET_COLUMNS if c not in targ_cols]
        if missing_in_target:
            print(f"WARNING: {args.schema}.{args.target_table} missing columns: {missing_in_target}")
    except Exception as e:
        print(f"ERROR inspecting target table {args.schema}.{args.target_table}: {e}")
        return 1

    any_rows = False

    for ym in months:
        # 1) Fetch + parse
        try:
            df_raw = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        if df_raw.empty:
            print(f"{ym}: no rows (zip parsed empty). Skipping.")
            continue

        # 2) Normalise & coerce types
        df = normalise_columns(df_raw)

        # Trim to target set (drop any extra cols the feed may include)
        df = df[TARGET_COLUMNS]

        # 3) Stage table → write
        staging = make_staging_name(args.target_table)
        try:
            create_staging_table(engine, args.schema, staging)
            written = write_stage_with_retry(engine, args.schema, staging, df)
            # 4) MERGE into target
            updated, inserted = merge_into_target(engine, args.schema, staging, args.target_table)
            print(f"✅ {ym}: staged {written:,} → merged (ins={inserted:,}, upd={updated:,}) into {args.schema}.{args.target_table}")
            any_rows = any_rows or written > 0
        except Exception as e:
            print(f"{ym}: SQL stage/merge error: {e}")
            return 1
        finally:
            # Best-effort cleanup
            try:
                drop_table_if_exists(engine, args.schema, staging)
            except Exception:
                pass

    if not any_rows:
        print("Nothing ingested.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
