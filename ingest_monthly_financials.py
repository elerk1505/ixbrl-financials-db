#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (robust upsert)

- Choose months via range/list/lookback flags.
- Parse the Companies House monthly iXBRL ZIP (live/archived).
- Normalise & align to the exact dbo.financials schema (39 columns).
- Load into dbo.financials_stage then MERGE into dbo.financials on
  (companies_house_registered_number, date) to avoid PK violations.
- Minimal logging: per-month summary with insert/update counts.

Requires: httpx, pandas, stream-read-xbrl, sqlalchemy, pyodbc
"""

from __future__ import annotations
import argparse
import os
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple, Dict

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# ------------- Constants (final table columns, order) -----------------

FINANCIALS_COLS: List[str] = [
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

PK_LEFT = "companies_house_registered_number"
PK_RIGHT = "date"

# ------------- SQL helpers -----------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    # fast_executemany for speed; future=True for 2.0 style
    return create_engine(engine_url, fast_executemany=True, future=True)

def ensure_staging_table(engine: Engine, schema: str, target: str, staging: str) -> None:
    # Create dbo.financials_stage (structure cloned from target) if missing
    sql = f"""
IF OBJECT_ID(N'[{schema}].[{staging}]', N'U') IS NULL
BEGIN
    SELECT TOP 0 *
    INTO [{schema}].[{staging}]
    FROM [{schema}].[{target}];
END
ELSE
BEGIN
    TRUNCATE TABLE [{schema}].[{staging}];
END
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def merge_stage_into_target(engine: Engine, schema: str, staging: str, target: str) -> Tuple[int, int]:
    """
    MERGE staging -> target on (companies_house_registered_number, date).
    Returns (inserted_count, updated_count).
    """
    # We collect $action into a temp table and return counts.
    cols_ins = ", ".join(f"[{c}]" for c in FINANCIALS_COLS)
    cols_src = ", ".join(f"S.[{c}]" for c in FINANCIALS_COLS)

    sql = f"""
DECLARE @Ops TABLE(Action nvarchar(10));
MERGE [{schema}].[{target}] AS T
USING (SELECT * FROM [{schema}].[{staging}]) AS S
   ON (T.[{PK_LEFT}] = S.[{PK_LEFT}] AND T.[{PK_RIGHT}] = S.[{PK_RIGHT}])
WHEN MATCHED THEN UPDATE SET
    {", ".join(f"T.[{c}] = S.[{c}]" for c in FINANCIALS_COLS if c not in (PK_LEFT, PK_RIGHT))}
WHEN NOT MATCHED THEN
    INSERT ({cols_ins})
    VALUES ({cols_src})
OUTPUT $action INTO @Ops;

SELECT
    SUM(CASE WHEN Action='INSERT' THEN 1 ELSE 0 END) AS inserted,
    SUM(CASE WHEN Action='UPDATE' THEN 1 ELSE 0 END) AS updated
FROM @Ops;
"""
    with engine.begin() as conn:
        res = conn.exec_driver_sql(sql).fetchone()
        inserted = int(res[0] or 0)
        updated = int(res[1] or 0)
        # Clear stage after merge
        conn.exec_driver_sql(f"TRUNCATE TABLE [{schema}].[{staging}];")
    return inserted, updated

# ------------- Month selection helpers -----------------

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
        if m == 0: y, m = y - 1, 12
    return out

# ------------- Download & parse the monthly ZIP -----------------

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # 'January', etc.

def candidate_urls_for_month(ym: str) -> List[str]:
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
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
            df = pd.DataFrame(data, columns=columns)
            df["zip_url"] = url
            return df
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"No monthly file at {url} (404). Trying next…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            break
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# ------------- Normalisation & alignment -----------------

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create/clean the columns we need for the final table.
    """
    df = dedupe_columns(df)

    # company number
    if "companies_house_registered_number" in df.columns:
        df["companies_house_registered_number"] = (
            df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )
    elif "company_number" in df.columns:
        df["companies_house_registered_number"] = (
            df["company_number"].astype(str).str.replace(" ", "", regex=False)
        )

    # date-ish fields → datetime (later cast to DATE in SQL)
    def _coerce_date(colnames: Sequence[str]) -> Optional[str]:
        for c in colnames:
            if c in df.columns:
                s = pd.to_datetime(df[c], errors="coerce")
                df[c] = s.dt.to_pydatetime()
                return c
        return None

    # Prefer explicit 'date' / 'balance_sheet_date' / 'period_end'
    _coerce_date(["date"])
    _coerce_date(["balance_sheet_date"])
    # Sometimes period_end/balance_sheet_date are alternates—keep both if present.
    if "period_end" not in df.columns:
        cand = _coerce_date(["period_end", "date_end", "yearEnd"])
        if cand and cand != "period_end":
            df = df.rename(columns={cand: "period_end"})

    # booleans to 0/1 (company_dormant sometimes "True"/"False")
    if "company_dormant" in df.columns:
        s = df["company_dormant"].astype(str).str.lower()
        df["company_dormant"] = s.isin(["true", "1", "t", "yes", "y"]).astype("Int64").fillna(0)

    # numeric-ish columns: coerce to numbers (keep NaN as None later)
    numeric_cols = set(FINANCIALS_COLS) - {
        "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date",
        "companies_house_registered_number", "entity_current_legal_name",
        "period_start", "period_end", "error", "zip_url",
    }
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # company_id: if absent, reuse CHRN (some datasets split these)
    if "company_id" not in df.columns and "companies_house_registered_number" in df.columns:
        df["company_id"] = df["companies_house_registered_number"]

    # default run_code/file_type/taxonomy if missing
    df["run_code"] = df.get("run_code", pd.Series(["Prod"] * len(df)))
    df["file_type"] = df.get("file_type", pd.Series(["html"] * len(df)))
    df["taxonomy"] = df.get("taxonomy", pd.Series([""] * len(df)))

    # ensure error column exists
    if "error" not in df.columns:
        df["error"] = ""

    return df

def align_to_final(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep exactly FINANCIALS_COLS; create missing ones with None.
    Also ensure empty strings become None for numeric/date fields.
    """
    # Add any missing columns
    for c in FINANCIALS_COLS:
        if c not in df.columns:
            df[c] = None

    # Reorder
    df = df[FINANCIALS_COLS].copy()

    # Empty strings -> None (avoids 22018 cast errors)
    for c in FINANCIALS_COLS:
        if df[c].dtype == object:
            df.loc[df[c].astype(str).str.len() == 0, c] = None

    return df

# ------------- Load (stage -> merge) -----------------

def write_stage(engine: Engine, schema: str, staging: str, df: pd.DataFrame) -> int:
    """
    Bulk insert into staging with executemany for reliability.
    We pass Python datetimes (SQL Server DATE will accept the date part).
    """
    if df.empty:
        return 0

    placeholders = ", ".join(["?"] * len(FINANCIALS_COLS))
    collist = ", ".join(f"[{c}]" for c in FINANCIALS_COLS)
    insert_sql = f"INSERT INTO [{schema}].[{staging}] ({collist}) VALUES ({placeholders})"

    # Convert pandas NaN to None so SQL Server sees NULL
    rows = []
    for _, r in df.iterrows():
        row = []
        for c in FINANCIALS_COLS:
            v = r[c]
            if pd.isna(v):
                row.append(None)
            else:
                row.append(v)
        rows.append(tuple(row))

    with engine.begin() as conn:
        raw = conn.connection  # DBAPI connection
        cur = raw.cursor()
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)
    return len(rows)

# ------------- Main -----------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (upsert).")
    # Range / list / lookback
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
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
    engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)

    # Sanity: target has (at least) our columns
    insp = inspect(engine)
    try:
        tgt_cols = [c["name"] for c in insp.get_columns(args.target_table, schema=args.schema)]
    except Exception as e:
        print(f"ERROR: cannot inspect {args.schema}.{args.target_table}: {e}")
        return 1

    missing = [c for c in FINANCIALS_COLS if c not in tgt_cols]
    if missing:
        print(f"ERROR: target table {args.schema}.{args.target_table} is missing columns: {missing}")
        return 1

    # Ensure staging table exists, then truncate each run
    ensure_staging_table(engine, args.schema, args.target_table, args.staging_table)

    any_rows = False
    for ym in months:
        try:
            df_raw = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch error: {e}")
            return 1
        if df_raw.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # Normalize & align
        df_norm = normalise_columns(df_raw)
        df_final = align_to_final(df_norm)

        # Load -> stage
        staged = write_stage(engine, args.schema, args.staging_table, df_final)
        if staged == 0:
            print(f"{ym}: nothing to stage. Skipping.")
            continue

        # MERGE
        inserted, updated = merge_stage_into_target(engine, args.schema, args.staging_table, args.target_table)
        print(f"✅ {ym}: staged {staged:,} | inserted {inserted:,} | updated {updated:,}")
        any_rows = any_rows or (inserted + updated) > 0

    if not any_rows:
        print("Nothing ingested (no new or changed rows).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
