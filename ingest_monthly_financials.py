#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Companies House iXBRL monthly -> Azure SQL (robust: stage + merge + retries + rollback)

Usage examples:
  python ingest_monthly_financials.py --since-months 1 --odbc-connect "DRIVER=...;SERVER=...;DATABASE=...;UID=...;PWD=..."
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 --odbc-connect "<your odbc>"

What this script does:
  1) For each month, download the official iXBRL monthly ZIP (live, then archive fallback).
  2) Stream-parse rows with `stream_read_xbrl_zip` (no files saved).
  3) Stage rows into [schema].[financials_stage] (ALL columns NULLable).
  4) Upsert into [schema].[financials] using MERGE on (companies_house_registered_number, date).
  5) Cleans stage and moves to next month. Retries transient failures with rollback.

Safe defaults:
  - All stage columns NVARCHAR(MAX) NULL so stage never fails on missing data.
  - We coerce empty strings -> NULL before staging.
  - DB ops use retries + explicit rollback on any error.

Primary key assumption for MERGE:
  (companies_house_registered_number, date)
If your target PK differs, adjust MERGE ON accordingly.
"""

from __future__ import annotations

import argparse
import calendar
import contextlib
import os
import sys
import time
from datetime import date, datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import httpx
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Optional (used only for a tiny helper; feel free to remove pandas if you prefer)
try:
    import pandas as pd  # noqa: F401
except Exception:  # pragma: no cover
    pd = None  # type: ignore


# ---------- Configuration ----------

# The final table column order (as you confirmed)
FINAL_COLS: List[str] = [
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

# Staging table name (in the same schema as the target)
DEFAULT_STAGE = "financials_stage"

# Retries
HTTP_RETRIES = 5
HTTP_RETRY_SLEEP = 6
SQL_RETRIES = 4
SQL_RETRY_SLEEP = 6

BATCH_SIZE = 1000  # rows per executemany to stage


# ---------- Helpers: dates & months ----------

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
    # current month back N-1 months
    today = datetime.now(timezone.utc).date().replace(day=1)
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(out))


def month_name_en(ym: str) -> str:
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]


# ---------- Data source URLs ----------

def candidate_urls_for_month(ym: str) -> List[str]:
    """
    Return URL candidates newest->older.
    Official pattern:
      Live    : https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{Month}{YYYY}.zip
      Archive : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{Month}{YYYY}.zip
      Special : JanuaryToDecember 2008/2009
    """
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


# ---------- HTTP fetch & parse (streaming) ----------

@contextlib.contextmanager
def _http_stream(url: str, timeout: float = 180.0):
    """
    Stream an HTTP response with retries and content-length validation.
    Yields a context whose .iter_bytes() can be passed to stream_read_xbrl_zip.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", "0") or "0")
                # Wrap to count bytes we actually iterate (for validation)
                total = 0

                def gen() -> Iterator[bytes]:
                    nonlocal total
                    for chunk in r.iter_bytes():
                        total += len(chunk)
                        yield chunk

                class _Wrapper:
                    def iter_bytes(self) -> Iterator[bytes]:
                        return gen()

                wrapper = _Wrapper()
                try:
                    yield wrapper
                finally:
                    # Validate size if Content-Length was provided
                    if expected and total != expected:
                        raise IOError(
                            f"incomplete body: got {total} expected {expected}"
                        )
                return
        except Exception as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                print(f"  fetch attempt {attempt}/{HTTP_RETRIES} failed: {e}")
                time.sleep(HTTP_RETRY_SLEEP)
                continue
            raise

    if last_err:
        raise last_err


def stream_rows_from_zip(url: str, timeout: float = 180.0) -> Tuple[List[str], Iterator[List[str]]]:
    """
    Returns (columns, iterator of row-lists as strings).
    """
    try:
        from stream_read_xbrl import stream_read_xbrl_zip
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency 'stream-read-xbrl'. "
            "Install with: pip install stream-read-xbrl"
        ) from e

    with _http_stream(url, timeout=timeout) as wrapper:
        # stream_read_xbrl_zip expects an iterator of bytes; pass wrapper.iter_bytes()
        with stream_read_xbrl_zip(wrapper.iter_bytes()) as (columns, rows):
            # Normalise column names to exact strings (no spaces trimmed here; we map later)
            return list(columns), rows


# ---------- SQL / Engine ----------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError(
            "Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC)."
        )
    if not engine_url:
        import urllib.parse

        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)  # type: ignore[arg-type]
    # fast_executemany will be enabled on the DBAPI cursor below
    return create_engine(engine_url, future=True)


def ensure_stage_table(engine: Engine, schema: str, stage_table: str):
    """
    Create stage table if it doesn't exist.
    ALL COLUMNS NULLABLE NVARCHAR(MAX) so staging never fails on NULLs.
    """
    col_defs = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in FINAL_COLS)
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :stage
)
BEGIN
    EXEC('CREATE TABLE [{schema}].[{stage}] (
{col_defs}
    )');
END
""".replace(":schema", "'"+schema+"'").replace(":stage", "'"+stage_table+"'")
    # (We pre-bind schema/stage above because we EXEC dynamic SQL)
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def truncate_stage(engine: Engine, schema: str, stage_table: str):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE [{schema}].[{stage_table}]")


def merge_stage_into_target(
    engine: Engine,
    schema: str,
    stage_table: str,
    target_table: str,
) -> int:
    """
    Upsert from stage -> target using MERGE on (companies_house_registered_number, date).
    Returns number of rows affected (inserted + updated).
    """
    set_cols = [c for c in FINAL_COLS if c not in ("companies_house_registered_number", "date")]
    set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in set_cols)

    # Use a temp table to capture OUTPUT $action to count affected rows
    sql = f"""
DECLARE @changes TABLE (action NVARCHAR(10));
MERGE [{schema}].[{target_table}] AS t
USING (SELECT * FROM [{schema}].[{stage_table}]) AS s
  ON t.[companies_house_registered_number] = s.[companies_house_registered_number]
 AND t.[date] = s.[date]
WHEN MATCHED THEN
  UPDATE SET {set_clause}
WHEN NOT MATCHED BY TARGET THEN
  INSERT ({", ".join(f"[{c}]" for c in FINAL_COLS)})
  VALUES ({", ".join("s.["+c+"]" for c in FINAL_COLS)})
OUTPUT $action INTO @changes;
SELECT COUNT(*) FROM @changes;
"""
    with engine.begin() as conn:
        rows = conn.exec_driver_sql(sql).scalar_one()
    return int(rows or 0)


# ---------- Row shaping & staging ----------

def _normalize_value(v):
    # Empty strings -> NULL
    if v is None:
        return None
    if isinstance(v, str):
        v = v.strip()
        return v if v != "" else None
    # Datetime/date -> ISO 8601 (SQL Server will cast NVARCHAR -> DATE/DATETIME)
    if isinstance(v, (datetime, date)):
        # Preserve full datetime if present
        if isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        return v.strftime("%Y-%m-%d")
    # bool -> 0/1 string
    if isinstance(v, bool):
        return "1" if v else "0"
    # else stringify to avoid cast issues in stage
    return str(v)


def _row_to_stage_list(in_cols: List[str], row: Sequence, zip_url: str) -> List[Optional[str]]:
    """
    Map the parsed row to the FINAL_COLS order, normalising values.
    Unseen columns are NULL; we also ensure 'zip_url' is filled.
    """
    # Build a dict quickly
    record: Dict[str, Optional[str]] = {}
    for c, v in zip(in_cols, row):
        record[c] = _normalize_value(v)

    out: List[Optional[str]] = []
    for col in FINAL_COLS:
        if col == "zip_url":
            out.append(zip_url)
        else:
            out.append(record.get(col))
    return out


def stage_rows(
    engine: Engine,
    schema: str,
    stage_table: str,
    in_cols: List[str],
    rows_iter: Iterator[Sequence],
    zip_url: str,
) -> int:
    """
    Insert rows into stage table in batches using fast_executemany.
    Returns number of staged rows.
    """
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        try:
            cur.fast_executemany = True  # pyodbc optimization
        except Exception:
            pass

        placeholders = ", ".join("?" for _ in FINAL_COLS)
        insert_sql = f"INSERT INTO [{schema}].[{stage_table}] ({', '.join('['+c+']' for c in FINAL_COLS)}) VALUES ({placeholders})"

        batch: List[List[Optional[str]]] = []
        total = 0

        def flush():
            nonlocal total
            if not batch:
                return
            for attempt in range(1, SQL_RETRIES + 1):
                try:
                    cur.executemany(insert_sql, batch)
                    raw.commit()
                    total += len(batch)
                    batch.clear()
                    return
                except Exception as e:
                    raw.rollback()
                    if attempt == SQL_RETRIES:
                        raise
                    print(f"    stage executemany retry {attempt}/{SQL_RETRIES} after error: {e}")
                    time.sleep(SQL_RETRY_SLEEP)

        for row in rows_iter:
            batch.append(_row_to_stage_list(in_cols, row, zip_url))
            if len(batch) >= BATCH_SIZE:
                flush()

        flush()
        return total
    finally:
        try:
            raw.close()
        except Exception:
            pass


# ---------- Main pipeline ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest Companies House iXBRL monthly zips into Azure SQL (stage+merge).")
    # Month selection
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months", help="Comma-separated list, e.g. 2025-01,2025-02")
    ap.add_argument("--since-months", type=int, default=1)
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default=DEFAULT_STAGE)
    # Network / timeouts
    ap.add_argument("--timeout", type=float, default=240.0)

    args = ap.parse_args()

    # Resolve months
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = [m.strip() for m in args.months.split(",") if m.strip()]
    else:
        months = default_since_months(args.since_months)

    print(f"*** --schema {args.schema} --target-table {args.target_table}")
    print(f"Target months: {months}")

    # Engine / connection
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR: failed to build engine: {e}")
        return 1

    # Ensure stage exists, and target table has expected columns
    try:
        ensure_stage_table(engine, args.schema, args.staging_table)
        insp = inspect(engine)
        tgt_cols = [c["name"] for c in insp.get_columns(args.target_table, schema=args.schema)]
        missing = [c for c in FINAL_COLS if c not in tgt_cols]
        if missing:
            print(f"ERROR: target {args.schema}.{args.target_table} is missing columns: {missing}")
            return 1
    except Exception as e:
        print(f"ERROR: schema inspection / ensure stage failed: {e}")
        return 1

    total_ingested = 0

    for ym in months:
        # 1) Find a working URL
        urls = candidate_urls_for_month(ym)
        columns: Optional[List[str]] = None
        rows_iter: Optional[Iterator[List[str]]] = None
        chosen_url: Optional[str] = None
        for url in urls:
            print(f"Fetching month: {ym} -> {url}")
            try:
                columns, rows_iter = stream_rows_from_zip(url, timeout=args.timeout)
                chosen_url = url
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    print(f"  {ym}: 404 at {url}, trying next URL…")
                    continue
                print(f"  {ym}: fetch/parse error: {e}")
                columns = None
                rows_iter = None
                break
            except Exception as e:
                print(f"  {ym}: fetch/parse error: {e}")
                columns = None
                rows_iter = None
                break

        if not columns or rows_iter is None or chosen_url is None:
            print(f"{ym}: no available monthly ZIP (skipping).")
            continue

        # 2) Stage in batches with retries + rollback
        # First clean stage for this month
        try:
            truncate_stage(engine, args.schema, args.staging_table)
        except Exception as e:
            print(f"{ym}: staging cleanup error: {e}")
            return 1

        staged_rows = 0
        try:
            staged_rows = stage_rows(
                engine,
                args.schema,
                args.staging_table,
                columns,
                rows_iter,
                chosen_url,
            )
        except Exception as e:
            print(f"{ym}: staging error: {e}")
            # Ensure stage is clean so next month isn't affected
            with contextlib.suppress(Exception):
                truncate_stage(engine, args.schema, args.staging_table)
            continue

        # 3) MERGE stage -> target with retries
        merged = 0
        last_err: Optional[Exception] = None
        for attempt in range(1, SQL_RETRIES + 1):
            try:
                merged = merge_stage_into_target(engine, args.schema, args.staging_table, args.target_table)
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"  {ym}: merge attempt {attempt}/{SQL_RETRIES} failed: {e}")
                time.sleep(SQL_RETRY_SLEEP)

        if last_err:
            print(f"{ym}: staging/merge error: {last_err}")
            # Don't abort entire run; continue with next month
            with contextlib.suppress(Exception):
                truncate_stage(engine, args.schema, args.staging_table)
            continue

        # 4) Clean stage again (best-effort)
        with contextlib.suppress(Exception):
            truncate_stage(engine, args.schema, args.staging_table)

        print(f"{ym}: staged {staged_rows:,} rows • merged {merged:,} rows.")
        total_ingested += merged

    print(f"Ingest complete. Rows ingested: {total_ingested:,}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
