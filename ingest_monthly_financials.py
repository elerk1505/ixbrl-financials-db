#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (robust loader)

- Select months by range/list/lookback
- Download with retries + content-length validation
- Parse iXBRL monthly ZIP
- Stage as NVARCHAR(MAX), then MERGE (UPSERT) into dbo.financials
- Converts types with TRY_CONVERT so empty strings become NULL
- Silent per-row output

Assumptions:
- Primary key (or natural key) is (companies_house_registered_number, date).
  Adjust MERGE ON clause if your PK differs.

Requires: httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import os
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Dict

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# -----------------------------
# Constants (final target columns, in order)
# -----------------------------

FINAL_COLUMNS: List[str] = [
    "run_code","company_id","date","file_type","taxonomy","balance_sheet_date",
    "companies_house_registered_number","entity_current_legal_name","company_dormant",
    "average_number_employees_during_period","period_start","period_end",
    "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
    "creditors_due_within_one_year","creditors_due_after_one_year","net_current_assets_liabilities",
    "total_assets_less_current_liabilities","net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
    "turnover_gross_operating_revenue","other_operating_income","cost_sales",
    "gross_profit_loss","administrative_expenses","raw_materials_consumables",
    "staff_costs","depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2","operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
    "error","zip_url",
]

# Columns by target type for MERGE conversions
DATE_COLS = {"date","balance_sheet_date","period_start","period_end"}
BIT_COLS = {"company_dormant"}
# Everything numeric except the string-ish columns below
STRING_COLS = {
    "run_code","company_id","file_type","taxonomy",
    "companies_house_registered_number","entity_current_legal_name","error","zip_url"
}
NUM_COLS = set(FINAL_COLUMNS) - DATE_COLS - BIT_COLS - STRING_COLS

# -----------------------------
# Helpers
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    """
    Build SQLAlchemy engine. If using ODBC connect string, add retry-friendly params
    unless already present.
    """
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")

    if not engine_url:
        import urllib.parse
        # Add ConnectRetry if not provided
        if "ConnectRetryCount" not in odbc_connect:
            odbc_connect += ";ConnectRetryCount=3;ConnectRetryInterval=10"
        # Strongly recommended for Azure SQL
        if "Encrypt" not in odbc_connect:
            odbc_connect += ";Encrypt=yes"
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    return create_engine(engine_url, fast_executemany=True, future=True)

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

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]

def candidate_urls_for_month(ym: str) -> List[str]:
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls

# -----------------------------
# Download + Parse
# -----------------------------

def stream_zip_with_retries(url: str, timeout: float, max_attempts: int = 5, backoff: int = 8):
    """
    Yields byte chunks from the URL. Retries on partial/incomplete response (peer closed, etc).
    Validates Content-Length if present.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", "0") or "0")
                total = 0
                for chunk in r.iter_bytes():
                    total += len(chunk)
                    yield chunk
                # Validate size if header present
                if expected and total != expected:
                    raise IOError(
                        f"Incomplete body: received {total} bytes, expected {expected}"
                    )
                return
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                print(f"[download retry {attempt}/{max_attempts}] {type(e).__name__}: {e}")
                time.sleep(backoff)
                continue
            break
    if last_err:
        raise last_err

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> pd.DataFrame:
    """
    Try each candidate URL for the month; stream and parse with stream_read_xbrl_zip.
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            def gen():
                for chunk in stream_zip_with_retries(url, timeout=timeout):
                    yield chunk
            with stream_read_xbrl_zip(gen()) as (columns, rows):
                data = [[("" if v is None else str(v)) for v in row] for row in rows]
            df = pd.DataFrame(data, columns=columns)
            # Drop duplicate-named columns if any (keep first)
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()].copy()
            return df
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"No monthly file at {url} (404). Trying next candidate…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            # For non-404 (e.g., partial body), stop trying this URL but try the next one
            print(f"{yyyy_mm}: fetch/parse error: {e}")
            # continue to next url
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# -----------------------------
# Normalisation (minimal, schema-safe)
# -----------------------------

def normalise_df_columns(df: pd.DataFrame, ym: str) -> pd.DataFrame:
    """
    Map/derive only fields needed for the final table:
      - companies_house_registered_number -> normalised (strip spaces)
      - derive 'date' if needed from candidate columns (balance sheet date often equals period_end)
      - coerce 'period_end' from various header variants
      - add static run_code, zip_url
    Unknown columns are ignored; missing target columns are created as None.
    """
    # company number
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"]
    elif "company_number" in df.columns:
        s = df["company_number"]
    else:
        s = pd.Series([None] * len(df))
    company_no = s.astype(str).str.replace(" ", "", regex=False)
    df["companies_house_registered_number"] = company_no

    # date candidates
    date_candidates = []
    for c in ("date", "balance_sheet_date", "period_end", "yearEnd", "date_end"):
        if c in df.columns:
            date_candidates.append(c)
    picked_date_col = date_candidates[0] if date_candidates else None
    if picked_date_col:
        df["date"] = pd.to_datetime(df[picked_date_col], errors="coerce")

    # standardise period_end alias
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")

    # ensure required strings exist
    df["run_code"] = f"Prod{datetime.utcnow().strftime('%y%m')}"
    df["zip_url"] = None  # set later per-month

    # make sure all final columns exist
    for c in FINAL_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # return only final columns
    out = df[FINAL_COLUMNS].copy()
    return out

# -----------------------------
# SQL: staging + merge
# -----------------------------

def ensure_staging_table(engine: Engine, schema: str, staging_table: str):
    """
    Create staging table with all columns NVARCHAR(MAX) if it doesn't exist.
    """
    cols_sql = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in FINAL_COLUMNS)
    sql = f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :table
)
BEGIN
    EXEC('CREATE TABLE [{schema}].[{staging_table}] (
        {cols_sql}
    )');
END
    """.replace(":schema", schema).replace(":table", staging_table)
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def truncate_staging(engine: Engine, schema: str, staging_table: str):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE [{schema}].[{staging_table}]")

def write_stage(engine: Engine, schema: str, staging_table: str, df: pd.DataFrame) -> int:
    """
    Stage all rows as NVARCHAR(MAX). Cast here to str or None to avoid dtype surprises.
    """
    if df.empty:
        return 0
    # Convert pandas types to either str or None so NVARCHAR insert is clean
    df2 = df.copy()
    for c in df2.columns:
        s = df2[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            # convert to ISO strings (SQL will TRY_CONVERT back)
            df2[c] = s.dt.to_pydatetime()
        else:
            df2[c] = s.where(s.notna(), None)
            # leave numeric as Python numbers or strings—pyodbc will bind fine to NVARCHAR
            df2[c] = df2[c].apply(lambda v: None if v is None else str(v))
    # Load in chunks using raw executemany for speed & lower memory
    cols = ", ".join(f"[{c}]" for c in df2.columns)
    placeholders = ", ".join("?" for _ in df2.columns)
    insert_sql = f"INSERT INTO [{schema}].[{staging_table}] ({cols}) VALUES ({placeholders})"
    rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]
    total = 0
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            with engine.begin() as conn:
                cur = conn.connection.cursor()
                cur.fast_executemany = True
                cur.executemany(insert_sql, rows)
                total = cur.rowcount if cur.rowcount is not None else len(rows)
                break
        except Exception as e:
            if attempt == max_retries:
                raise
            print(f"[stage retry {attempt}/{max_retries}] {e}")
            time.sleep(5)
    return total

def merge_stage_into_final(engine: Engine, schema: str, staging_table: str, final_table: str) -> int:
    """
    MERGE from staging into final.
    - ON (companies_house_registered_number, date)
    - TRY_CONVERT for dates/numerics; BIT via CASE
    - Update all non-key columns when matched
    Returns rows affected (insert + update).
    """
    # Build SELECT list with conversions
    def src_expr(c: str) -> str:
        if c in DATE_COLS:
            return f"TRY_CONVERT(DATETIME2, NULLIF(s.[{c}], ''))"
        if c in BIT_COLS:
            # accept 'True'/'False', '1'/'0', 'true'/'false'
            return f"CASE WHEN LOWER(NULLIF(s.[{c}], '')) IN ('1','true') THEN 1 WHEN LOWER(NULLIF(s.[{c}], '')) IN ('0','false') THEN 0 ELSE NULL END"
        if c in NUM_COLS:
            return f"TRY_CONVERT(DECIMAL(38,6), NULLIF(s.[{c}], ''))"
        return f"s.[{c}]"

    src_list = ",\n        ".join(f"{src_expr(c)} AS [{c}]" for c in FINAL_COLUMNS)

    # Columns to insert
    insert_cols = ", ".join(f"[{c}]" for c in FINAL_COLUMNS)
    insert_vals = ", ".join(f"src.[{c}]" for c in FINAL_COLUMNS)

    # Columns to update (exclude ON keys)
    on_keys = ["companies_house_registered_number", "date"]
    update_cols = [c for c in FINAL_COLUMNS if c not in on_keys]
    update_set = ",\n        ".join(f"t.[{c}] = src.[{c}]" for c in update_cols)

    merge_sql = f"""
MERGE [{schema}].[{final_table}] AS t
USING (
    SELECT
        {src_list}
    FROM [{schema}].[{staging_table}] s
) AS src
ON (t.[companies_house_registered_number] = src.[companies_house_registered_number]
    AND t.[date] = src.[date])
WHEN MATCHED THEN
    UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals});
"""
    with engine.begin() as conn:
        res = conn.exec_driver_sql(merge_sql)
        # SQL Server doesn't always return rowcount for MERGE reliably; best-effort:
        try:
            return res.rowcount
        except Exception:
            return 0

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (robust).")
    # Range / list / lookback
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials", help="Final table (default: financials)")
    ap.add_argument("--staging-table", default="financials_stage", help="Staging table (NVARCHAR)")
    # Networking / robustness
    ap.add_argument("--timeout", type=float, default=300.0, help="HTTP timeout seconds")
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

    # Ensure staging exists
    ensure_staging_table(engine, args.schema, args.staging_table)

    wrote_any = False
    for ym in months:
        # 1) Fetch & parse
        try:
            df = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1
        if df.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # 2) Normalise to final schema columns only
        df = normalise_df_columns(df, ym)
        df["zip_url"] = candidate_urls_for_month(ym)[0]  # record the primary attempt URL

        # 3) Stage
        try:
            truncate_staging(engine, args.schema, args.staging_table)
            staged = write_stage(engine, args.schema, args.staging_table, df)
            print(f"{ym}: staged {staged} rows.")
        except Exception as e:
            print(f"{ym}: staging error: {e}")
            return 1

        # 4) MERGE (UPSERT) into final
        try:
            affected = merge_stage_into_final(engine, args.schema, args.staging_table, args.target_table)
            print(f"✅ {ym}: upserted rows (reported): {affected}")
            wrote_any = True
        except Exception as e:
            print(f"{ym}: MERGE error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
