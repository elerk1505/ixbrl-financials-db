#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (Companies House monthly ZIPs)

Choose months via:
  --start-month YYYY-MM --end-month YYYY-MM    (inclusive)
  --months 2024-01,2024-02
  --since-months N   (default 1)

Table schema (39 columns, in order):
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
tax_on_profit_or_loss_on_ordinary_activities, profit_loss_for_period, error, zip_url
"""

from __future__ import annotations
import argparse
import os
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# -----------------------------
# Target table definition (order matters)
# -----------------------------
TARGET_COLS: List[str] = [
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
# Everything numeric that isn’t date/bool/string-ish:
NUMERIC_COLS = {
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
# The remaining columns are treated as strings.
STRING_COLS = set(TARGET_COLS) - DATE_COLS - BOOL_COLS - NUMERIC_COLS

# -----------------------------
# Small utils
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

# -----------------------------
# SQL helpers
# -----------------------------
def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    return create_engine(engine_url, fast_executemany=True, future=True)

def to_sql_append(df: pd.DataFrame, engine: Engine, table: str, schema: str = "dbo", chunksize: int = 1000) -> int:
    if df.empty:
        return 0
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=chunksize,
        method=None,
    )
    return len(df)

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]

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
# Download & parse the monthly ZIP
# -----------------------------
def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # 'January', ...

def candidate_urls_for_month(ym: str) -> List[str]:
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> tuple[pd.DataFrame, str]:
    from stream_read_xbrl import stream_read_xbrl_zip
    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                    data = [[("" if v is None else str(v)) for v in row] for row in rows]
            df = dedupe_columns(pd.DataFrame(data, columns=columns))
            return df, url
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"No monthly file at {url} (404). Trying next candidate…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            break
    if last_err:
        raise last_err
    raise FileNotFoundError(f"No URL worked for {yyyy_mm}")

# -----------------------------
# Normalisation / sanitisation
# -----------------------------
def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Canonical company_number from CH registered number
    if "companies_house_registered_number" in df.columns:
        s = series_or_first(df, "companies_house_registered_number")
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Canonicalise period_end column name
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Parse dates if present (we'll re-coerce later as well)
    for col in ("date", "balance_sheet_date", "period_start", "period_end"):
        if col in df.columns:
            s = series_or_first(df, col)
            df[col] = pd.to_datetime(s, errors="coerce").dt.date

    return df

def align_df_to_target(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only the columns that exist in TARGET_COLS (plus add any missing)
    keep = [c for c in df.columns if c in TARGET_COLS]
    out = df[keep].copy()
    for col in TARGET_COLS:
        if col not in out.columns:
            out[col] = pd.NA
    # Reorder
    out = out[TARGET_COLS]
    return out

def coerce_bool(series: pd.Series) -> pd.Series:
    # Accept True/False, 'true'/'false', '1'/'0', '' -> NA
    s = series.astype(str).str.strip().str.lower()
    mapped = s.map({"true": 1, "false": 0, "1": 1, "0": 0, "": pd.NA})
    # if something else slips through, set NA
    mapped = mapped.where(mapped.isin([0, 1]) | mapped.isna(), other=pd.NA)
    return mapped.astype("Int64")

def sanitise_for_schema(df: pd.DataFrame, zip_url: str) -> pd.DataFrame:
    # 1) Ensure string columns are strings (but keep NA as NA)
    for col in STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    # 2) Dates -> datetime.date (NaT -> NA)
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # 3) Bools -> 0/1 (nullable Int64 so NULLs stay NULL)
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = coerce_bool(df[col])

    # 4) Numeric -> to_numeric ('' -> NaN -> NULL)
    for col in NUMERIC_COLS:
        if col in df.columns:
            # first, treat empty strings and blanks as NA
            df[col] = df[col].replace({"": pd.NA, " ": pd.NA})
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) Companies number: strip spaces again for safety
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"].astype("string")
        df["companies_house_registered_number"] = s.str.replace(" ", "", regex=False)

    # 6) Add/overwrite zip_url and ensure error exists
    df["zip_url"] = zip_url
    if "error" not in df.columns:
        df["error"] = pd.NA

    return df

# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL.")
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive).")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive).")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 months.")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--append-retries", type=int, default=5)

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

    # Optionally verify table exists and (soft) matches columns
    try:
        existing_cols = get_table_columns(engine, args.schema, args.target_table)
        if not existing_cols:
            print(f"ERROR: target table {args.schema}.{args.target_table} not found or has no columns.")
            return 1
        print(f"Detected {args.schema}.{args.target_table} columns: {len(existing_cols)}")
    except Exception as e:
        print(f"ERROR inspecting {args.schema}.{args.target_table}: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # 1) Fetch & parse (and remember which URL worked)
        try:
            df_raw, url_used = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"Fetch/parse error for {ym}: {e}")
            return 1

        if df_raw.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # 2) Normalise raw -> tidy
        df_norm = normalise_columns(df_raw)

        # 3) Align to target schema and add missing columns
        df_aligned = align_df_to_target(df_norm)

        # 4) Final sanitation for SQL types & provenance
        df_final = sanitise_for_schema(df_aligned, zip_url=url_used)

        if df_final.empty:
            print(f"{ym}: nothing to append after alignment/sanitisation. Skipping.")
            continue

        # 5) Append with retries
        last_err: Optional[Exception] = None
        for attempt in range(1, args.append_retries + 1):
            try:
                rows = to_sql_append(
                    df_final, engine, table=args.target_table, schema=args.schema, chunksize=1000
                )
                print(f"✅ {ym}: appended {rows} rows into {args.schema}.{args.target_table}.")
                wrote_any = True
                last_err = None
                break
            except SQLAlchemyError as e:
                last_err = e
                print(f"[append attempt {attempt}/{args.append_retries}] OperationalError: {e}")
            except Exception as e:
                last_err = e
                print(f"[append attempt {attempt}/{args.append_retries}] Unexpected SQL error: {e}")
            import time
            time.sleep(5)

        if last_err:
            print(f"SQL error for {ym} into {args.schema}.{args.target_table}: {last_err}")
            return 1

    if not wrote_any:
        print("Nothing ingested (no files or all empty).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
