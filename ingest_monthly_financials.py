#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (robust, idempotent, quiet logs)

Choose months via:
  A) Range   : --start-month YYYY-MM --end-month YYYY-MM  (inclusive)
  B) List    : --months 2024-01,2024-02
  C) Lookback: --since-months N  (N=1 => current month only)

Destination:
- Writes rows into a STAGING table first (default: dbo.financials_stage).
- Then MERGE/UPSERTs into TARGET table (default: dbo.financials)
  using KEY: (companies_house_registered_number, date)
  (matches the PK seen in your SQL error logs).

Resilience:
- Download retries with content-length verification.
- Uses correct monthly URL + archive fallback.
- Skips month cleanly if all candidates 404.
- SQL operations wrapped in short transactions with rollback and retries.
- Avoid noisy per-row prints.

Columns (kept exactly; missing values allowed):
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

Requires: httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import calendar
import os
import sys
import time
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# -----------------------------
# Constants
# -----------------------------

COLUMNS = [
    "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date",
    "companies_house_registered_number", "entity_current_legal_name", "company_dormant",
    "average_number_employees_during_period", "period_start", "period_end",
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year",
    "net_current_assets_liabilities", "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability", "called_up_share_capital",
    "profit_loss_account_reserve", "shareholder_funds", "turnover_gross_operating_revenue",
    "other_operating_income", "cost_sales", "gross_profit_loss", "administrative_expenses",
    "raw_materials_consumables", "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
    "error", "zip_url"
]

# Key used for the upsert (this matches your PK_financials errors)
UPSERT_KEYS = ("companies_house_registered_number", "date")


# -----------------------------
# Utilities
# -----------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


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
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # 'January', 'February', ...


# -----------------------------
# Companies House ZIP URLs
# -----------------------------

def candidate_urls_for_month(ym: str) -> List[str]:
    """
    Correct URL order:
      1) Live monthly
      2) Archive monthly
    """
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    return [live, archive]


# -----------------------------
# HTTP download & parse (robust)
# -----------------------------

def _fetch_stream(url: str, timeout: float) -> Tuple[httpx.Response, Optional[int]]:
    headers = {"Accept": "application/zip"}
    r = httpx.stream("GET", url, timeout=timeout, headers=headers)
    r.__enter__()  # manual context mgmt so caller can r.__exit__()
    try:
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        expected = int(cl) if cl and cl.isdigit() else None
        return r, expected
    except Exception:
        r.__exit__(None, None, None)
        raise


def fetch_month_zip_as_rows(yyyy_mm: str, timeout: float = 180.0, max_attempts: int = 5):
    """
    Returns (columns, rows_iterable) using stream_read_xbrl_zip on a verified, complete body.
    Retries on partial/incomplete bodies and on transient network issues.
    Skips month (returns None) if ALL candidate URLs are 404.
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    for url in candidate_urls_for_month(yyyy_mm):
        log(f"Fetching month: {yyyy_mm} -> {url}")
        # Attempt up to max_attempts for incomplete bodies
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                r, expected = _fetch_stream(url, timeout=timeout)
                received = 0
                # We pass the byte generator to stream_read_xbrl_zip
                def gen():
                    nonlocal received
                    for chunk in r.iter_bytes():
                        if chunk:
                            received += len(chunk)
                            yield chunk
                # Let parser consume the generator
                with stream_read_xbrl_zip(gen()) as (columns, rows):
                    # If expected size known, ensure we read all bytes
                    if expected is not None and received != expected:
                        raise IOError(f"incomplete body: got {received} expected {expected}")
                    # success
                    r.__exit__(None, None, None)
                    return columns, rows, url
            except httpx.HTTPStatusError as e:
                # 404 => try next candidate URL
                if e.response is not None and e.response.status_code == 404:
                    log(f"  {yyyy_mm}: 404 at {url}, trying next URL…")
                    last_exc = e
                    break  # next candidate url
                last_exc = e
            except Exception as e:
                last_exc = e
                log(f"  {yyyy_mm}: fetch attempt {attempt}/{max_attempts} failed: {e}")
                time.sleep(3)
            finally:
                try:
                    r.__exit__(None, None, None)  # if open
                except Exception:
                    pass
        if last_exc:
            # If 404 we proceed to next candidate; other errors we also try next
            continue

    # If we reached here, all candidates failed (likely 404s)
    log(f"{yyyy_mm}: no available monthly ZIP (skipping).")
    return None


# -----------------------------
# DataFrame shaping
# -----------------------------

def df_from_rows(columns: List[str], rows_iter) -> pd.DataFrame:
    # Stream rows into list of lists; normalize None->'' and cast to str (then re-parse dates)
    data = []
    for row in rows_iter:
        data.append([("" if v is None else str(v)) for v in row])
    df = pd.DataFrame(data, columns=columns)

    # Keep only our target columns if they exist; add missing ones as empty
    # (the monthly files may vary)
    out = pd.DataFrame({c: (df[c] if c in df.columns else "") for c in COLUMNS})

    # Basic cleanups: strip spaces in CH numbers, parse dates
    if "companies_house_registered_number" in out.columns:
        out["companies_house_registered_number"] = (
            out["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )

    # Prefer 'date' if present; otherwise derive from balance_sheet_date or period_end
    def _to_dt(s: pd.Series) -> pd.Series:
        return pd.to_datetime(s, errors="coerce").dt.date

    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        out[c] = _to_dt(out[c])

    # If date is missing, try fallback columns
    fill_date = out["date"].isna()
    if fill_date.any():
        out.loc[fill_date, "date"] = out.loc[fill_date, "balance_sheet_date"]
        fill_date = out["date"].isna()
    if fill_date.any():
        out.loc[fill_date, "date"] = out.loc[fill_date, "period_end"]

    # company_id: if present in feed, keep; else default to companies_house_registered_number
    if out["company_id"].isna().all() or (out["company_id"] == "").all():
        out["company_id"] = out["companies_house_registered_number"]

    return out


# -----------------------------
# SQL helpers
# -----------------------------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    """
    Accepts either a full SQLAlchemy URL or a raw ODBC connect string.
    Adds sane defaults for transient faults.
    """
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")

    if not engine_url:
        import urllib.parse
        # Add optional stability knobs to ODBC string if absent
        defaults = [
            ("Encrypt", "yes"),
            ("TrustServerCertificate", "no"),
            ("ConnectRetryCount", "5"),
            ("ConnectRetryInterval", "5"),
            ("Connection Timeout", "45"),
            ("MARS_Connection", "Yes"),
        ]
        for k, v in defaults:
            if f"{k}=" not in odbc_connect:
                sep = ";" if odbc_connect and not odbc_connect.endswith(";") else ""
                odbc_connect = f"{odbc_connect}{sep}{k}={v}"
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)

    return create_engine(engine_url, fast_executemany=True, future=True, pool_pre_ping=True)


def ensure_staging_table(engine: Engine, schema: str, staging_table: str):
    """
    Create a wide NVARCHAR(MAX) staging table if it doesn't exist (idempotent).
    """
    colspec = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in COLUMNS)
    sql = text("""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :table
)
BEGIN
    EXEC(N'CREATE TABLE [' + :schema + N'].[' + :table + N'] (
""" + colspec + """
    )');
END
""")
    with engine.begin() as conn:
        conn.execute(sql, {"schema": schema, "table": staging_table})


def staging_delete_month(engine: Engine, schema: str, staging_table: str, ym: str):
    """
    Clear previous rows for the same ZIP URL (idempotency for re-runs).
    """
    mon = month_name_en(ym)
    y = parse_yyyy_mm(ym).year
    zip_like = f"Accounts_Monthly_Data-{mon}{y}.zip"
    sql = text(f"DELETE FROM [{schema}].[{staging_table}] WHERE [zip_url] LIKE :z")
    with engine.begin() as conn:
        conn.execute(sql, {"z": f"%{zip_like}%"})


def write_stage(engine: Engine, schema: str, staging_table: str, df: pd.DataFrame) -> int:
    """
    Append to staging using NVARCHAR(MAX) for strings/dates (let MERGE coerce).
    """
    if df.empty:
        return 0
    # Everything as text to avoid type surprises in staging
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]) or pd.api.types.is_object_dtype(df2[c]):
            pass
        else:
            # force to string to avoid dtype churn
            df2[c] = df2[c].astype(str)
    # Ensure the exact column order
    df2 = df2[COLUMNS]

    # Use pandas to_sql (append) inside a small retry wrapper
    from sqlalchemy import types as T
    dtype = {c: T.NVARCHAR(length=None) for c in COLUMNS}

    attempts = 5
    for attempt in range(1, attempts + 1):
        try:
            df2.to_sql(staging_table, engine, schema=schema, if_exists="append", index=False, dtype=dtype, chunksize=1000)
            return len(df2)
        except SQLAlchemyError as e:
            log(f"  staging append attempt {attempt}/{attempts} failed: {e}")
            if attempt == attempts:
                raise
            time.sleep(3)
    return 0


def merge_stage_into_target(engine: Engine, schema: str, staging_table: str, target_table: str):
    """
    MERGE/UPSERT from staging into target on (companies_house_registered_number, date).
    Updates non-key columns when matched, inserts when not matched.
    """
    key1, key2 = UPSERT_KEYS
    cols = COLUMNS[:]  # copy
    # Ensure keys exist
    if key1 not in cols or key2 not in cols:
        raise RuntimeError("Upsert key columns missing from COLUMNS definition.")

    # Build SET list excluding keys
    set_cols = [c for c in cols if c not in UPSERT_KEYS]
    set_clause = ", ".join(f"T.[{c}] = S.[{c}]" for c in set_cols)

    insert_cols = ", ".join(f"[{c}]" for c in cols)
    insert_vals = ", ".join(f"S.[{c}]" for c in cols)

    sql = f"""
MERGE [{schema}].[{target_table}] AS T
USING (SELECT * FROM [{schema}].[{staging_table}]) AS S
ON (T.[{key1}] = S.[{key1}] AND T.[{key2}] = S.[{key2}])
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
"""
    attempts = 5
    for attempt in range(1, attempts + 1):
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(sql)
            return
        except SQLAlchemyError as e:
            log(f"  merge attempt {attempt}/{attempts} failed: {e}")
            if attempt == attempts:
                raise
            time.sleep(5)


def cleanup_staging(engine: Engine, schema: str, staging_table: str):
    """
    Optional: remove staged rows older than 14 days to keep it tidy.
    """
    sql = text(f"DELETE FROM [{schema}].[{staging_table}] WHERE TRY_CONVERT(datetime, [date]) < DATEADD(day, -14, SYSUTCDATETIME())")
    try:
        with engine.begin() as conn:
            conn.execute(sql)
    except Exception:
        # best-effort
        pass


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (robust upsert).")
    # Range / list / lookback
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months")
    ap.add_argument("--since-months", type=int, default=1)

    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")

    # Network / retries
    ap.add_argument("--timeout", type=float, default=180.0)

    args = ap.parse_args()

    # Build month list
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    log(f"Target months: {months}")

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        log(f"ERROR building engine: {e}")
        return 1

    # Ensure staging exists
    try:
        ensure_staging_table(engine, args.schema, args.staging_table)
    except Exception as e:
        log(f"ERROR ensuring staging table: {e}")
        return 1

    total_rows = 0
    any_success = False

    for ym in months:
        # 1) Fetch + parse
        result = None
        try:
            result = fetch_month_zip_as_rows(ym, timeout=args.timeout, max_attempts=5)
        except Exception as e:
            log(f"{ym}: fetch/parse error: {e}")
            continue

        if not result:
            # No file for that month — benign
            continue

        columns, rows, zip_url = result

        # 2) DataFrame build + annotate
        try:
            df = df_from_rows(columns, rows)
            df["zip_url"] = zip_url
            df["run_code"] = f"Prod224_3500"  # keep same style as your previous logs
            # keep 'error' column blank for successful rows
            if "error" in df.columns:
                df["error"] = df["error"].fillna("")
        except Exception as e:
            log(f"{ym}: dataframe build error: {e}")
            continue

        if df.empty:
            log(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # 3) Staging: remove same-zip rows (idempotent) then append
        try:
            staging_delete_month(engine, args.schema, args.staging_table, ym)
            wrote = write_stage(engine, args.schema, args.staging_table, df)
            log(f"{ym}: staged {wrote} rows.")
        except SQLAlchemyError as e:
            log(f"{ym}: staging SQL error: {e}")
            return 1
        except Exception as e:
            log(f"{ym}: staging unexpected error: {e}")
            return 1

        # 4) MERGE/UPSERT
        try:
            merge_stage_into_target(engine, args.schema, args.staging_table, args.target_table)
            any_success = True
            total_rows += len(df)
            log(f"{ym}: merge upsert OK.")
        except SQLAlchemyError as e:
            log(f"{ym}: staging/merge error: {e}")
            return 1
        except Exception as e:
            log(f"{ym}: unexpected merge error: {e}")
            return 1

    if any_success:
        cleanup_staging(engine, args.schema, args.staging_table)
        log(f"✅ Done. Upserted rows (count includes duplicates overwritten): ~{total_rows}.")
    else:
        log("Nothing ingested (no files found or all months empty).")

    return 0


if __name__ == "__main__":
    sys.exit(main())
