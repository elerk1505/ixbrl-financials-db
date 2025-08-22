#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Companies House iXBRL -> Azure SQL (monthly ZIPs)

Usage examples:
  # Range inclusive
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 --odbc-connect "<ODBC STRING>" --schema dbo --target-table financials

  # Lookback (N months incl. current)
  python ingest_monthly_financials.py --since-months 1 --odbc-connect "<ODBC STRING>"

Notes
- Tries both live and archive URLs:
    https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{Month}{YYYY}.zip
    https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{Month}{YYYY}.zip
  and yearly bundle for 2008/2009.
- Parses iXBRL entries via stream-read-xbrl (stream_read_xbrl_zip).
- Loads to a staging table NVARCHAR(MAX) then MERGEs into target with upsert semantics.
- Default natural key for MERGE: (companies_house_registered_number, period_end).
  You can change with --merge-keys "companies_house_registered_number,period_end,file_type"

Columns expected (39):
run_code, company_id, date, file_type, taxonomy, balance_sheet_date,
companies_house_registered_number, entity_current_legal_name, company_dormant,
average_number_employees_during_period, period_start, period_end, tangible_fixed_assets,
debtors, cash_bank_in_hand, current_assets, creditors_due_within_one_year,
creditors_due_after_one_year, net_current_assets_liabilities,
total_assets_less_current_liabilities, net_assets_liabilities_including_pension_asset_liability,
called_up_share_capital, profit_loss_account_reserve, shareholder_funds,
turnover_gross_operating_revenue, other_operating_income, cost_sales, gross_profit_loss,
administrative_expenses, raw_materials_consumables, staff_costs,
depreciation_other_amounts_written_off_tangible_intangible_fixed_assets,
other_operating_charges_format2, operating_profit_loss,
profit_loss_on_ordinary_activities_before_tax,
tax_on_profit_or_loss_on_ordinary_activities, profit_loss_for_period, error, zip_url
"""

from __future__ import annotations

import argparse
import calendar
import os
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# --------------------------------------------------------------------
# Config / constants
# --------------------------------------------------------------------

TARGET_COLUMNS = [
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

DEFAULT_MERGE_KEYS = ["companies_house_registered_number", "period_end"]


# --------------------------------------------------------------------
# Helpers: months & URLs
# --------------------------------------------------------------------

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


def default_since_months(n: int) -> List[str]:
    today = datetime.now(timezone.utc).date().replace(day=1)
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(out))


def month_name_en(ym: string) -> str:
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]


def candidate_urls_for_month(ym: str) -> List[str]:
    """Return URL candidates from newest to older locations."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(
            f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip"
        )
    return urls


# --------------------------------------------------------------------
# HTTP fetch with robust retries + Content-Length validation
# --------------------------------------------------------------------

class IncompleteBody(Exception):
    pass


def fetch_bytes_with_retries(url: str, timeout: float = 180.0, attempts: int = 5) -> bytes:
    """
    Robust download: validates Content-Length (if present), retries on short reads,
    network hiccups, and 5xx. Uses httpx streaming; collects into bytes for the
    iXBRL streaming reader.
    """
    last_err: Optional[Exception] = None
    headers = {"User-Agent": "ixbrl-financials-ingest/1.0"}
    for attempt in range(1, attempts + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout, headers=headers) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length") or 0)
                chunks = []
                total = 0
                for chunk in r.iter_bytes():
                    if chunk:
                        chunks.append(chunk)
                        total += len(chunk)
                body = b"".join(chunks)
                if expected and total != expected:
                    raise IncompleteBody(f"incomplete body: got {total} expected {expected}")
                return body
        except (httpx.HTTPError, IncompleteBody) as e:
            last_err = e
            # For 404, don't retry more than once on same URL; caller will try next candidate
            is_404 = isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 404
            if is_404:
                break
            if attempt < attempts:
                print(f"      fetch attempt {attempt}/{attempts} failed: {e!s}")
                time.sleep(3 * attempt)
            else:
                break
    if last_err:
        raise last_err
    raise RuntimeError("Unknown fetch failure")


# --------------------------------------------------------------------
# iXBRL ZIP parsing
# --------------------------------------------------------------------

def fetch_month_zip_as_df(yyyy_mm: str, timeout: float = 180.0) -> pd.DataFrame:
    """
    Opens the monthly ZIP and streams iXBRL entries to rows using stream_read_xbrl_zip.
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        print(f"Fetching month: {yyyy_mm} -> {url}")
        try:
            body = fetch_bytes_with_retries(url, timeout=timeout, attempts=5)
            with stream_read_xbrl_zip(body) as (columns, rows):
                # rows are tuples; coerce None -> ""
                data = [[("" if v is None else str(v)) for v in row] for row in rows]
            df = pd.DataFrame(data, columns=columns)
            if df.empty:
                print(f"  {yyyy_mm}: ZIP had no rows, skipping.")
                return pd.DataFrame()
            df["zip_url"] = url
            return df
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"  {yyyy_mm}: 404 at {url}, trying next URL…")
                last_err = e
                continue
            raise
        except Exception as e:
            last_err = e
            print(f"  {yyyy_mm}: fetch/parse error: {e!s}")
            break

    if last_err:
        raise last_err
    return pd.DataFrame()


# --------------------------------------------------------------------
# Data normalisation for target columns
# --------------------------------------------------------------------

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df


def series_or_first(df: pd.DataFrame, name: str) -> pd.Series:
    obj = df[name]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Canonical company_number (from CH registered number)
    if "companies_house_registered_number" in df.columns:
        s = series_or_first(df, "companies_house_registered_number")
        df["companies_house_registered_number"] = (
            s.astype(str).str.replace(" ", "", regex=False)
        )

    # Canonicalise 'period_end' and 'date'/'balance_sheet_date'
    # Input tags differ; unify to period_end as date
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    if "period_end" in df.columns:
        s = series_or_first(df, "period_end")
        df["period_end"] = pd.to_datetime(s, errors="coerce").dt.date

    for c in ("date", "balance_sheet_date", "period_start"):
        if c in df.columns:
            s = series_or_first(df, c)
            # Keep as python datetime.date — to_sql with NVARCHAR so we'll stringify
            df[c] = pd.to_datetime(s, errors="coerce").dt.to_pydatetime()

    # Ensure all TARGET_COLUMNS exist; fill missing with ""
    for col in TARGET_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Reorder to target columns only
    df_out = df[TARGET_COLUMNS].copy()

    # Stringify datetimes to ISO for NVARCHAR staging
    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        if c in df_out.columns:
            s = df_out[c]
            if pd.api.types.is_datetime64_any_dtype(s) or s.dtype == "object":
                df_out[c] = pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")

    return df_out


# --------------------------------------------------------------------
# SQL helpers
# --------------------------------------------------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    # pool_pre_ping to avoid stale connections; small pool to be gentle in CI
    eng = create_engine(
        engine_url,
        pool_pre_ping=True,
        pool_recycle=180,
        future=True,
        fast_executemany=True,
    )
    return eng


def ensure_staging_table(engine: Engine, schema: str, staging_table: str):
    cols_sql = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in TARGET_COLUMNS)
    # IMPORTANT: schema/table names must be compared as strings, not identifiers
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
""".replace(":schema", "'"+schema+"'").replace(":table", "'"+staging_table+"'")
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]


@contextmanager
def begin_conn(engine: Engine):
    """
    Context manager that provides a connection with explicit transaction.
    Ensures rollback on error to avoid 'invalid transaction' state.
    """
    conn = engine.connect()
    trans = conn.begin()
    try:
        yield conn
        trans.commit()
    except Exception:
        try:
            trans.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def stage_rows(engine: Engine, schema: str, staging_table: str, df: pd.DataFrame) -> int:
    """
    Append DataFrame to NVARCHAR staging with retries.
    """
    if df.empty:
        return 0

    # Rename columns to safe identifiers
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})

    # pandas to_sql: supply dtype of NVARCHAR(MAX)
    from sqlalchemy import types as T
    dtypes = {c: T.NVARCHAR(length=None) for c in df.columns}

    attempts = 4
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            df.to_sql(
                name=staging_table,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                dtype=dtypes,
                chunksize=2000,
                method=None,
            )
            return len(df)
        except Exception as e:
            last_err = e
            if attempt < attempts:
                print(f"    staging append attempt {attempt}/{attempts} failed: {e!s}")
                time.sleep(2 * attempt)
            else:
                break
    if last_err:
        raise last_err
    return 0


def merge_from_staging(
    engine: Engine,
    schema: str,
    staging_table: str,
    target_table: str,
    merge_keys: Sequence[str],
) -> int:
    """
    MERGE NVARCHAR staging -> target table (assumes target columns exist).
    Upsert: when matched, UPDATE (set all non-key columns); when not matched, INSERT.

    Returns number of rows affected (from MERGE output).
    """
    key_expr = " AND ".join([f"T.[{k}] = S.[{k}]" for k in merge_keys])
    nonkey_cols = [c for c in TARGET_COLUMNS if c not in merge_keys]
    set_expr = ", ".join([f"T.[{c}] = S.[{c}]" for c in nonkey_cols])
    insert_cols = ", ".join(f"[{c}]" for c in TARGET_COLUMNS)
    insert_vals = ", ".join(f"S.[{c}]" for c in TARGET_COLUMNS)

    sql = f"""
SET NOCOUNT ON;
MERGE [{schema}].[{target_table}] AS T
USING (SELECT {", ".join("[" + c + "]" for c in TARGET_COLUMNS)} FROM [{schema}].[{staging_table}]) AS S
ON ({key_expr})
WHEN MATCHED THEN UPDATE SET {set_expr}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
SELECT @@ROWCOUNT AS rc;
"""
    attempts = 4
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            with begin_conn(engine) as conn:
                res = conn.exec_driver_sql(sql).scalar()
                return int(res or 0)
        except SQLAlchemyError as e:
            last_err = e
            if attempt < attempts:
                print(f"    MERGE attempt {attempt}/{attempts} failed: {e!s}")
                time.sleep(2 * attempt)
            else:
                break
    if last_err:
        raise last_err
    return 0


def clear_staging(engine: Engine, schema: str, staging_table: str):
    sql = f"TRUNCATE TABLE [{schema}].[{staging_table}]"
    attempts = 3
    last_err: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            with begin_conn(engine) as conn:
                conn.exec_driver_sql(sql)
            return
        except SQLAlchemyError as e:
            last_err = e
            if attempt < attempts:
                print(f"    staging cleanup attempt {attempt}/{attempts} failed: {e!s}")
                time.sleep(2 * attempt)
            else:
                break
    if last_err:
        raise last_err


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest Companies House MONTHLY iXBRL into Azure SQL.")
    # Months
    ap.add_argument("--start-month", help="YYYY-MM start (inclusive)")
    ap.add_argument("--end-month", help="YYYY-MM end (inclusive)")
    ap.add_argument("--months", help="Comma-separated YYYY-MM (e.g., 2025-01,2025-02)")
    ap.add_argument("--since-months", type=int, default=1, help="If no months given, current month back N-1 (default 1)")
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
    ap.add_argument("--merge-keys", default=",".join(DEFAULT_MERGE_KEYS), help="Comma-separated key columns for upsert")
    # Network
    ap.add_argument("--timeout", type=float, default=180.0)
    # Misc
    ap.add_argument("--quiet", action="store_true", help="Less chatty output")

    args = ap.parse_args()
    merge_keys = [k.strip() for k in args.merge_keys.split(",") if k.strip()]

    # Build months list
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = [x.strip() for x in args.months.split(",") if x.strip()]
    else:
        months = default_since_months(args.since_months)

    if not args.quiet:
        print(f"Target months: {months}")

    # Build engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    # Ensure staging exists and target columns look sensible
    try:
        ensure_staging_table(engine, args.schema, args.staging_table)
    except Exception as e:
        print(f"ERROR ensuring staging table: {e}")
        return 1

    try:
        tgt_cols = set(get_table_columns(engine, args.schema, args.target_table))
        missing = [c for c in TARGET_COLUMNS if c not in tgt_cols]
        if missing:
            print(f"ERROR: target {args.schema}.{args.target_table} missing {len(missing)} cols, e.g. {missing[:5]}")
            return 1
    except Exception as e:
        print(f"ERROR inspecting target table: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # 0) Clear staging at start of each month (safe)
        try:
            clear_staging(engine, args.schema, args.staging_table)
        except Exception as e:
            print(f"{ym}: staging cleanup error: {e}")
            return 1

        # 1) Fetch + parse the monthly ZIP
        try:
            df_raw = fetch_month_zip_as_df(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            continue

        if df_raw.empty:
            if not args.quiet:
                print(f"{ym}: no rows parsed, skipping.")
            continue

        # 2) Normalise to target columns
        df_final = normalise_columns(df_raw)

        # 3) Stage rows
        try:
            staged = stage_rows(engine, args.schema, args.staging_table, df_final)
            if not args.quiet:
                print(f"{ym}: staged {staged} rows.")
        except Exception as e:
            print(f"{ym}: staging SQL error: {e}")
            return 1

        if staged == 0:
            continue

        # 4) MERGE -> target (upsert)
        try:
            affected = merge_from_staging(
                engine,
                args.schema,
                args.staging_table,
                args.target_table,
                merge_keys=merge_keys,
            )
            if not args.quiet:
                print(f"✅ {ym}: upserted {affected} rows into {args.schema}.{args.target_table}.")
            wrote_any = True
        except Exception as e:
            print(f"{ym}: staging/merge error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested (no files found or all months empty).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
