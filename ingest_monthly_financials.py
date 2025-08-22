#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Companies House MONTHLY iXBRL -> Azure SQL (resilient)

Usage examples:
  # explicit range
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 --odbc-connect "<pyodbc_connstr>" --schema dbo --target-table financials

  # last N months (N=1 => current month only)
  python ingest_monthly_financials.py --since-months 1 --odbc-connect "<pyodbc_connstr>"

Notes:
- Skips months that 404 (not yet published) without failing the whole run.
- Handles partial downloads with verify+resume.
- Upserts via MERGE on (companies_house_registered_number, period_end).
- Writes only concise progress logs (no per-row spam).
"""

from __future__ import annotations
import argparse
import calendar
import io
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple, Dict

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError

# external parser (as used previously)
from stream_read_xbrl import stream_read_xbrl_zip


# -----------------------------
# Config / constants
# -----------------------------

# Your full, desired target columns (we'll align/empty-fill as needed)
DESIRED_COLUMNS: List[str] = [
    "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date",
    "companies_house_registered_number", "entity_current_legal_name", "company_dormant",
    "average_number_employees_during_period", "period_start", "period_end", "tangible_fixed_assets",
    "debtors", "cash_bank_in_hand", "current_assets", "creditors_due_within_one_year",
    "creditors_due_after_one_year", "net_current_assets_liabilities", "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability", "called_up_share_capital",
    "profit_loss_account_reserve", "shareholder_funds", "turnover_gross_operating_revenue",
    "other_operating_income", "cost_sales", "gross_profit_loss", "administrative_expenses",
    "raw_materials_consumables", "staff_costs", "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss", "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period", "error", "zip_url",
]

HTTP_TIMEOUT = 180.0
HTTP_ATTEMPTS = 5
SQL_ATTEMPTS = 5
RESUME_CHUNK = 1024 * 1024  # 1MB
CHUNK_SIZE = 64 * 1024


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


def month_name_en(ym: str) -> str:
    y, m = ym.split("-", 1)
    return calendar.month_name[int(m)]


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
    """Companies House monthly ZIPs: live first, then archive; special 2008/2009 yearly bundles."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    live = f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip"
    archive = f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    urls = [live, archive]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls


# -----------------------------
# HTTP download with verify+resume
# -----------------------------

class IncompleteBody(Exception):
    pass


def _append_range(client: httpx.Client, url: str, fp, start: int, expected: Optional[int]) -> int:
    headers = {"Range": f"bytes={start}-"}
    with client.stream("GET", url, headers=headers, timeout=HTTP_TIMEOUT) as r:
        r.raise_for_status()
        for chunk in r.iter_bytes(CHUNK_SIZE):
            if chunk:
                fp.write(chunk)
    fp.flush()
    return os.fstat(fp.fileno()).st_size


def download_zip_with_retries(urls: List[str]) -> Optional[str]:
    """
    Try each URL in order; for each, attempt up to HTTP_ATTEMPTS with content-length verification
    and Range resume if short. Returns path to a complete ZIP file or None if 404 on all.
    """
    for url in urls:
        try:
            with httpx.Client(follow_redirects=True) as client:
                # initial GET to know content-length
                for attempt in range(1, HTTP_ATTEMPTS + 1):
                    try:
                        with client.stream("GET", url, timeout=HTTP_TIMEOUT) as r:
                            if r.status_code == 404:
                                # Try next candidate URL
                                break
                            r.raise_for_status()
                            expected = r.headers.get("Content-Length")
                            expected = int(expected) if (expected and expected.isdigit()) else None

                            # write to temp
                            fd, tmp = tempfile.mkstemp(prefix="ch_month_", suffix=".zip")
                            os.close(fd)
                            size = 0
                            try:
                                with open(tmp, "wb") as f:
                                    for chunk in r.iter_bytes(CHUNK_SIZE):
                                        if chunk:
                                            f.write(chunk)
                                size = os.path.getsize(tmp)

                                # verify
                                if expected is not None and size < expected:
                                    # try resume if server allows
                                    with open(tmp, "ab") as f:
                                        size = _append_range(client, url, f, size, expected)
                                # final check
                                if expected is not None and size != expected:
                                    raise IncompleteBody(f"incomplete body: got {size} expected {expected}")

                                # looks good
                                return tmp

                            except Exception:
                                # cleanup and retry
                                try:
                                    os.remove(tmp)
                                except Exception:
                                    pass
                                raise
                    except httpx.HTTPStatusError as e:
                        if e.response is not None and e.response.status_code == 404:
                            # Try next URL candidate
                            break
                        if attempt == HTTP_ATTEMPTS:
                            raise
                        time.sleep(3 * attempt)
                    except (httpx.TransportError, IncompleteBody) as e:
                        if attempt == HTTP_ATTEMPTS:
                            raise
                        time.sleep(3 * attempt)
                # next URL candidate
        except httpx.HTTPStatusError as e:
            # fatal (non-404)
            raise
        except Exception:
            # try next candidate
            continue
    # none worked (i.e., all 404 or failed)
    return None


def parse_zip_to_df_from_path(zip_path: str) -> pd.DataFrame:
    """Feed file bytes to stream_read_xbrl_zip (avoids closed network stream issues)."""
    def file_chunks(path: str, chunk: int = CHUNK_SIZE):
        with open(path, "rb") as f:
            while True:
                b = f.read(chunk)
                if not b:
                    break
                yield b

    with stream_read_xbrl_zip(file_chunks(zip_path)) as (columns, rows):
        data = [[("" if v is None else str(v)) for v in row] for row in rows]
    return dedupe_columns(pd.DataFrame(data, columns=columns))


# -----------------------------
# Normalisation
# -----------------------------

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Canonical company_number from CH registered number if present
    if "companies_house_registered_number" in df.columns:
        s = series_or_first(df, "companies_house_registered_number")
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Normalise period_end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Ensure period_end looks like a date string (target table likely has DATE)
    if "period_end" in df.columns:
        s = pd.to_datetime(series_or_first(df, "period_end"), errors="coerce")
        df["period_end"] = s.dt.strftime("%Y-%m-%d")

    return df


def align_and_fill(df: pd.DataFrame, required_cols: Sequence[str]) -> pd.DataFrame:
    """
    Keep only the columns we need in the target, add any missing with empty strings,
    and order them to exactly match the target order.
    """
    out = df.copy()
    # add missing
    for c in required_cols:
        if c not in out.columns:
            out[c] = ""
    # keep only required
    out = out[list(required_cols)].copy()
    return out


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
    # fast_executemany improves pandas to_sql
    return create_engine(engine_url, fast_executemany=True, future=True)


def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table, schema=schema)]
    if not cols:
        raise RuntimeError(f"Table {schema}.{table} not found or has no columns.")
    return cols


def ensure_staging_table(engine: Engine, schema: str, stage_table: str, cols: Sequence[str]) -> None:
    """
    Create a staging table with NVARCHAR(MAX) columns mirroring the target columns.
    """
    cols_spec = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in cols)
    sql_exists = text("""
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = :schema AND t.name = :tname
    )
    BEGIN
        EXEC(N'CREATE TABLE [' + :schema + N'].[' + :tname + N'] (
            """ + cols_spec + """
        )');
    END
    """)
    for attempt in range(1, SQL_ATTEMPTS + 1):
        try:
            with engine.begin() as conn:
                conn.execute(sql_exists, {"schema": schema, "tname": stage_table})
            return
        except SQLAlchemyError as e:
            if attempt == SQL_ATTEMPTS:
                raise
            time.sleep(3 * attempt)


def sql_retry(fn, *args, **kwargs):
    for attempt in range(1, SQL_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except OperationalError as e:
            # rollback and retry with backoff
            if attempt == SQL_ATTEMPTS:
                raise
            time.sleep(3 * attempt)
        except SQLAlchemyError:
            if attempt == SQL_ATTEMPTS:
                raise
            time.sleep(3 * attempt)


def clear_staging(engine: Engine, schema: str, stage_table: str):
    sql = text(f"DELETE FROM [{schema}].[{stage_table}]")
    sql_retry(lambda: engine.begin().__enter__().execute(sql))


def bulk_stage(engine: Engine, schema: str, stage_table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    # use pandas to_sql (append)
    for attempt in range(1, SQL_ATTEMPTS + 1):
        try:
            df.to_sql(
                name=stage_table,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
                dtype=None,  # we created NVARCHAR(MAX) earlier
                chunksize=2000,
                method=None,
            )
            return len(df)
        except SQLAlchemyError:
            if attempt == SQL_ATTEMPTS:
                raise
            time.sleep(3 * attempt)
    return 0


def merge_upsert(engine: Engine, schema: str, target_table: str, stage_table: str, cols: Sequence[str]) -> Tuple[int, int]:
    """
    MERGE from stage -> target on (companies_house_registered_number, period_end).
    Returns (updated_rows, inserted_rows).
    """
    key_a, key_b = "companies_house_registered_number", "period_end"
    if key_a not in cols or key_b not in cols:
        # Nothing to merge safely (shouldn't happen given your table); do nothing.
        return (0, 0)

    # All non-key columns
    non_keys = [c for c in cols if c not in (key_a, key_b)]
    set_list = ", ".join(f"T.[{c}] = S.[{c}]" for c in non_keys)
    insert_cols = ", ".join(f"[{c}]" for c in cols)
    values_cols = ", ".join(f"S.[{c}]" for c in cols)

    sql = f"""
    SET NOCOUNT ON;

    MERGE [{schema}].[{target_table}] AS T
    USING (
        SELECT {insert_cols}
        FROM [{schema}].[{stage_table}]
        WHERE [{key_a}] IS NOT NULL AND [{key_b}] IS NOT NULL
    ) AS S
    ON  T.[{key_a}] = S.[{key_a}] AND T.[{key_b}] = S.[{key_b}]
    WHEN MATCHED THEN
        UPDATE SET {set_list}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({values_cols})
    OUTPUT $action AS MergeAction;
    """

    updated = inserted = 0
    for attempt in range(1, SQL_ATTEMPTS + 1):
        try:
            with engine.begin() as conn:
                res = conn.exec_driver_sql(sql)
                # Count actions from OUTPUT
                for row in res:
                    if row[0] == "UPDATE":
                        updated += 1
                    elif row[0] == "INSERT":
                        inserted += 1
            break
        except SQLAlchemyError:
            if attempt == SQL_ATTEMPTS:
                raise
            time.sleep(3 * attempt)

    return updated, inserted


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Resilient ingest of MONTHLY iXBRL zips into Azure SQL.")
    # Month selection
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
    ap.add_argument("--timeout", type=float, default=HTTP_TIMEOUT)

    args = ap.parse_args()

    # Build month list
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)

    # Build engine
    engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)

    # Identify target schema/table columns
    try:
        target_cols = get_table_columns(engine, args.schema, args.target_table)
    except Exception as e:
        print(f"ERROR inspecting {args.schema}.{args.target_table}: {e}")
        return 1

    # Use intersection of DESIRED_COLUMNS and target table to drive shape
    col_order = [c for c in DESIRED_COLUMNS if c in target_cols]
    if not col_order:
        print(f"ERROR: target table {args.schema}.{args.target_table} has no overlap with DESIRED_COLUMNS.")
        return 1

    # Ensure staging table exists
    try:
        ensure_staging_table(engine, args.schema, args.staging_table, col_order)
    except Exception as e:
        print(f"ERROR ensuring staging table: {e}")
        return 1

    print(f"Target months: {months}")

    any_success = False

    for ym in months:
        urls = candidate_urls_for_month(ym)
        print(f"Fetching month: {ym} -> {urls[0]}")

        # 1) Download
        try:
            zip_path = download_zip_with_retries(urls)
            if not zip_path:
                print(f"{ym}: no ZIP available yet (404 on all candidates). Skipping.")
                continue
        except IncompleteBody as e:
            print(f"{ym}: download error: {e}. Skipping.")
            continue
        except httpx.HTTPError as e:
            print(f"{ym}: HTTP error: {e}. Skipping.")
            continue
        except Exception as e:
            print(f"{ym}: unexpected download error: {e}. Skipping.")
            continue

        # 2) Parse
        try:
            df = parse_zip_to_df_from_path(zip_path)
        except Exception as e:
            print(f"{ym}: parse error: {e}. Skipping.")
            try:
                os.remove(zip_path)
            except Exception:
                pass
            continue
        finally:
            try:
                os.remove(zip_path)
            except Exception:
                pass

        if df.empty:
            print(f"{ym}: parsed 0 rows. Skipping.")
            continue

        # 3) Normalise
        df = normalise_columns(df)
        # add metadata columns we control
        df["zip_url"] = urls[0]
        if "error" not in df.columns:
            df["error"] = ""

        # 4) Align to target shape
        df_final = align_and_fill(df, col_order)

        # 5) Stage -> MERGE
        try:
            # clear staging fully per-month (keeps transactions simple)
            clear_staging(engine, args.schema, args.staging_table)

            staged_rows = bulk_stage(engine, args.schema, args.staging_table, df_final)
            updated, inserted = merge_upsert(engine, args.schema, args.target_table, args.staging_table, col_order)

            print(f"✅ {ym}: staged {staged_rows:,} | upserted: {inserted:,} insert(s), {updated:,} update(s).")
            any_success = True
        except Exception as e:
            print(f"{ym}: staging/merge error: {e}")
            return 1

    if not any_success:
        print("Nothing ingested (no files or all months skipped/empty).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
