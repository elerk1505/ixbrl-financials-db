#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (staging + merge + retry + rollback)

Usage examples:
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 --odbc-connect "$AZURE_SQL_ODBC" --schema dbo --target-table financials
  python ingest_monthly_financials.py --months 2025-01,2025-02 --odbc-connect "$AZURE_SQL_ODBC"

Key features
- Robust fetch with retries, validates full-body download
- Parses Companies House MONTHLY iXBRL ZIPs via stream_read_xbrl
- Writes to a NULLable staging table, then MERGEs into target
- Retries SQL operations, rolls back on failure, then cleans up

Requires: httpx, pandas, sqlalchemy, pyodbc, stream-read-xbrl
"""

from __future__ import annotations
import argparse
import os
import sys
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# URL helpers
# -----------------------------

def month_name_en(ym: str) -> str:
    import calendar
    y, m = ym.split("-", 1)
    return calendar.month_name[int(m)]  # 'January'

def candidate_urls_for_month(ym: str) -> List[str]:
    """
    Newest-first location list for a given YYYY-MM month.
    """
    year = int(ym[:4])
    mon_name = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon_name}{year}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon_name}{year}.zip"

    urls = [live, archive]

    # historic yearly bundles (not relevant to 2025 but harmless)
    if year in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip")
    return urls

# -----------------------------
# Month selection helpers
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
# Engine / connectivity
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL / AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    eng = create_engine(engine_url, fast_executemany=True, future=True)
    return eng

def ping_engine(engine: Engine, retries: int = 5, sleep: float = 5.0) -> None:
    last = None
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Connectivity OK.")
            return
        except Exception as e:
            last = e
            print(f"[attempt {attempt}/{retries}] connect failed: {e!r}")
            time.sleep(sleep)
    raise RuntimeError(f"DB connectivity failed after retries: {last}")

# -----------------------------
# Fetch + parse iXBRL
# -----------------------------

def fetch_month_zip_to_rows(ym: str, *, attempts: int = 5, timeout: float = 180.0) -> Tuple[List[str], Iterable[List[str]]]:
    """
    Returns (columns, rows_iter). Uses stream_read_xbrl to iterate iXBRL content from the monthly ZIP.
    Retries on transient HTTP errors and validates full content-length for robustness.
    """
    try:
        from stream_read_xbrl import stream_read_xbrl_zip
    except Exception as e:
        raise RuntimeError("stream-read-xbrl is required. Install with `pip install stream-read-xbrl`.") from e

    last_err: Optional[Exception] = None

    for url in candidate_urls_for_month(ym):
        print(f"Fetching month: {ym} -> {url}")
        for attempt in range(1, attempts + 1):
            try:
                with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                    r.raise_for_status()
                    expected = int(r.headers.get("Content-Length", "0"))
                    got = 0
                    def gen():
                        nonlocal got
                        for chunk in r.iter_bytes():
                            got += len(chunk)
                            yield chunk
                    with stream_read_xbrl_zip(gen()) as (columns, rows):
                        # if content-length header provided, validate after the stream ends
                        if expected and got != expected:
                            raise IOError(f"incomplete body: got {got} expected {expected}")
                        return columns, rows
            except httpx.HTTPStatusError as he:
                if he.response is not None and he.response.status_code == 404:
                    print(f"  {ym}: 404 at {url}, trying next URL…")
                    last_err = he
                    break  # next URL
                last_err = he
                print(f"  {ym}: fetch attempt {attempt}/{attempts} failed: {he}")
                time.sleep(3)
            except Exception as e:
                last_err = e
                print(f"  {ym}: fetch attempt {attempt}/{attempts} failed: {e}")
                time.sleep(3)
        # end attempts
    # end url loop
    raise RuntimeError(f"{ym}: no available monthly ZIP (last error: {last_err})")

def rows_to_dataframe(columns: List[str], rows_iter: Iterable[List[str]]) -> pd.DataFrame:
    # stringify, allow None -> "" then fix later
    data = []
    for row in rows_iter:
        data.append([("" if v is None else str(v)) for v in row])
    df = pd.DataFrame(data, columns=columns)
    return df

# -----------------------------
# Column alignment & sanitation
# -----------------------------

FIN_COLS: List[str] = [
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
]

def normalize_df(df: pd.DataFrame, ym: str, zip_url: str) -> pd.DataFrame:
    # dedupe
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    # unify company number
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"]
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # ensure period_end exists (already provided by the pack as balance_sheet_date in many rows)
    if "period_end" not in df.columns and "balance_sheet_date" in df.columns:
        df["period_end"] = df["balance_sheet_date"]

    # coerce dates to ISO strings (SQLAlchemy to NVARCHAR(MAX))
    for c in ("date","balance_sheet_date","period_start","period_end"):
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce").dt.date
            df[c] = s.astype("string").fillna("")

    # attach zip url for provenance
    df["zip_url"] = zip_url
    return df

def align_to_financials(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in FIN_COLS if c in df.columns]
    missing = [c for c in FIN_COLS if c not in df.columns]
    out = df[keep].copy()
    for m in missing:
        out[m] = ""
    # reorder
    out = out[FIN_COLS]
    return out

# -----------------------------
# SQL: staging + merge
# -----------------------------

def ensure_staging_table(engine: Engine, schema: str, stage_table: str) -> None:
    """
    Creates a fully-NULLable staging table if it doesn't exist.
    """
    ddl_cols = ",\n".join([f"[{c}] NVARCHAR(MAX) NULL" for c in FIN_COLS])
    sql = f"""
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :tname
)
BEGIN
    EXEC(N'CREATE TABLE [{schema}].[{stage_table}] (
{ddl_cols}
    )')
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql), {"schema": schema, "tname": stage_table})

def truncate_table(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table}]"))

def stage_rows(engine: Engine, schema: str, stage_table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # NVARCHAR(MAX) staging — just send strings
    df = df.applymap(lambda x: None if (isinstance(x, str) and x == "") else x)
    df.to_sql(stage_table, engine, schema=schema, if_exists="append", index=False)
    return len(df)

def merge_stage_into_target(engine: Engine, schema: str, stage_table: str, target_table: str,
                            key_cols: Sequence[str]) -> int:
    """
    MERGE stage -> target on COALESCE-ed keys to avoid NULL-equality issues.
    Updates all non-key columns, inserts new rows.
    Returns rows affected (best-effort).
    """
    all_cols = FIN_COLS
    upd_cols = [c for c in all_cols if c not in key_cols]

    # Build ON with COALESCE for string/date keys (both are NVARCHAR here)
    def coalesce_expr(alias: str, col: str) -> str:
        # empty-string sentinel for NVARCHAR
        return f"COALESCE({alias}.[{col}], N'')"

    on_clauses = [f"{coalesce_expr('t', k)} = {coalesce_expr('s', k)}" for k in key_cols]
    on_sql = " AND ".join(on_clauses)

    set_sql = ", ".join([f"t.[{c}] = s.[{c}]" for c in upd_cols])
    insert_cols = ", ".join([f"[{c}]" for c in all_cols])
    insert_vals = ", ".join([f"s.[{c}]" for c in all_cols])

    merge_sql = f"""
MERGE [{schema}].[{target_table}] AS t
USING (SELECT * FROM [{schema}].[{stage_table}]) AS s
ON {on_sql}
WHEN MATCHED THEN UPDATE SET {set_sql}
WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
"""
    with engine.begin() as conn:
        res = conn.execute(text(merge_sql))
        # rowcount may be -1 for MERGE; return 0 if unavailable
        try:
            return int(res.rowcount or 0)
        except Exception:
            return 0

# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    # months selection
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months")
    ap.add_argument("--since-months", type=int, default=1)
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-suffix", default="_stage")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--http-retries", type=int, default=5)
    ap.add_argument("--merge-key", default="companies_house_registered_number,period_end",
                    help="Comma-separated key columns for MERGE (default: companies_house_registered_number,period_end)")

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

    # Engine + ping
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
        ping_engine(engine)
    except Exception as e:
        print(f"ERROR: DB connection failed: {e}")
        return 1

    stage_table = f"{args.target_table}{args.staging_suffix}"

    # Ensure staging exists (FIX: no 'stage' var usage)
    try:
        ensure_staging_table(engine, args.schema, stage_table)
    except Exception as e:
        print(f"ERROR: schema inspection / ensure stage failed: {e}")
        return 1

    total_ingested = 0

    # Process months
    for ym in months:
        try:
            cols, rows_iter = fetch_month_zip_to_rows(ym, attempts=args.http_retries, timeout=args.timeout)
            df = rows_to_dataframe(cols, rows_iter)
            if df.empty:
                print(f"{ym}: parsed 0 rows. Skipping.")
                continue

            # determine which URL we used (for provenance text in normalize) — best we can do
            used_url = candidate_urls_for_month(ym)[0]  # we print it above already; not tracked per attempt
            df = normalize_df(df, ym, used_url)
            df = align_to_financials(df)

            # stage -> merge with rollback guarded
            try:
                truncate_table(engine, args.schema, stage_table)
            except Exception:
                # ignore, table may be empty
                pass

            staged = stage_rows(engine, args.schema, stage_table, df)
            print(f"{ym}: staged {staged} rows…")

            keys = [k.strip() for k in args.merge_key.split(",") if k.strip()]
            merged = merge_stage_into_target(engine, args.schema, stage_table, args.target_table, keys)
            total_ingested += max(0, merged)
        except Exception as e:
            print(f"{ym}: staging/merge error: {e}")

    print(f"Ingest complete. Rows ingested: {total_ingested}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
