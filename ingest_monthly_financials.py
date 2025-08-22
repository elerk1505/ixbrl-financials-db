#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (resilient)

- Month selection: --start-month/--end-month OR --months=CSV OR --since-months N
- URLs:
    Live    : https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{Month}{YYYY}.zip
    Archive : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{Month}{YYYY}.zip
    Yearly  : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{YYYY}.zip (2008, 2009)

- Writes to <schema>.<target_table> via:
    1) write to <schema>.<target_table>_stage (NVARCHAR columns) in chunks with retries
    2) MERGE into final (WHEN NOT MATCHED THEN INSERT) to avoid PK dupe errors

Requires: httpx, pandas, sqlalchemy, pyodbc, stream-read-xbrl
"""

from __future__ import annotations
import argparse
import os
import sys
import time
import math
import re
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

# -----------------------------
# Config / constants
# -----------------------------

DEST_COLS: List[str] = [
    "run_code","company_id","date","file_type","taxonomy","balance_sheet_date",
    "companies_house_registered_number","entity_current_legal_name","company_dormant",
    "average_number_employees_during_period","period_start","period_end",
    "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
    "creditors_due_within_one_year","creditors_due_after_one_year",
    "net_current_assets_liabilities","total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
    "turnover_gross_operating_revenue","other_operating_income","cost_sales",
    "gross_profit_loss","administrative_expenses","raw_materials_consumables",
    "staff_costs","depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2","operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
    "error","zip_url",
]

# How many times to retry network/SQL steps
NET_RETRIES = 5
SQL_RETRIES = 5
RETRY_SLEEP = 5.0  # seconds

STAGE_CHUNK_ROWS = 5_000  # insert in smaller chunks to reduce transaction size


# -----------------------------
# Month helpers
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
# URLs (exact patterns you provided)
# -----------------------------

def month_name_en(ym: str) -> str:
    import calendar
    return calendar.month_name[int(ym[-2:])]  # 01 -> January

def candidate_urls_for_month(ym: str) -> List[str]:
    """Return URL candidates for this YYYY-MM based on CH patterns you shared."""
    y = int(ym[:4])
    mon = month_name_en(ym)  # 'January'
    candidates = [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip",
    ]
    # Some months are only in /archive/ (e.g., Dec 2024, Jan 2010, etc.)
    candidates.append(
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip"
    )
    # Yearly bundles for 2008/2009
    if y in (2008, 2009):
        candidates.append(
            f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip"
        )
    return candidates


# -----------------------------
# Networking (robust download)
# -----------------------------

def _content_length(resp: httpx.Response) -> Optional[int]:
    try:
        return int(resp.headers.get("Content-Length", ""))
    except Exception:
        return None

def fetch_month_zip_as_rows(yyyy_mm: str, timeout: float = 180.0) -> Tuple[List[str], Iterable[List[str]]]:
    """
    Download the monthly ZIP with retries, verify body length, stream-parse with stream_read_xbrl_zip.
    Returns (columns, rows_iterator).
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    last_err: Optional[Exception] = None
    for url in candidate_urls_for_month(yyyy_mm):
        for attempt in range(1, NET_RETRIES + 1):
            try:
                print(f"Fetching month: {yyyy_mm} -> {url}")
                with httpx.stream("GET", url, timeout=timeout, follow_redirects=True) as r:
                    if r.status_code == 404:
                        # Try next URL candidate
                        print(f"  {yyyy_mm}: 404 at {url}, trying next URL…")
                        last_err = httpx.HTTPStatusError("404", request=r.request, response=r)
                        break
                    r.raise_for_status()
                    # If server gives content-length, ensure we fully consume exactly that many bytes
                    expected = _content_length(r)
                    byte_iter = r.iter_bytes()
                    # Wrap for size-check
                    def sized_iter():
                        seen = 0
                        for chunk in byte_iter:
                            seen += len(chunk)
                            yield chunk
                        if expected is not None and seen != expected:
                            raise IOError(f"incomplete body: got {seen} expected {expected}")
                    with stream_read_xbrl_zip(sized_iter()) as (columns, rows):
                        # Return *generator* to keep memory small
                        return columns, rows
            except Exception as e:
                last_err = e
                if attempt < NET_RETRIES:
                    print(f"  {yyyy_mm}: fetch attempt {attempt}/{NET_RETRIES} failed: {e}; retrying in {RETRY_SLEEP}s…")
                    time.sleep(RETRY_SLEEP)
                else:
                    print(f"  {yyyy_mm}: fetch failed after {NET_RETRIES} attempts: {e}")
        # move to next candidate URL

    # If we got here, none worked
    raise last_err if last_err else RuntimeError(f"No URL worked for {yyyy_mm}")


# -----------------------------
# Data shaping
# -----------------------------

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def normalise_df(df: pd.DataFrame, zip_url: str) -> pd.DataFrame:
    df = dedupe_columns(df)

    # If "companies_house_registered_number" present, create a clean "company_number"
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        df["company_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Canonicalise period_end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break
    if "period_end" in df.columns:
        s = df["period_end"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        # keep as python datetime.date (SQLAlchemy handles it) – we’ll convert later if needed
        df["period_end"] = pd.to_datetime(s, errors="coerce")

    # Force only the destination columns (plus zip_url if not present)
    if "zip_url" not in df.columns:
        df["zip_url"] = zip_url

    # If the file provides a "company_number" but the target expects "companies_house_registered_number"
    if "company_number" in df.columns and "companies_house_registered_number" not in df.columns:
        df = df.rename(columns={"company_number": "companies_house_registered_number"})

    # Ensure all DEST_COLS exist, filling with None
    for c in DEST_COLS:
        if c not in df.columns:
            df[c] = None

    # Reorder
    df = df[DEST_COLS]

    # Types: keep as Python objects, change booleans to 0/1, datetimes to python datetime
    if "company_dormant" in df.columns:
        df["company_dormant"] = (
            df["company_dormant"]
            .astype(str)
            .str.lower()
            .map({"true": 1, "false": 0})
            .fillna(0)
        )

    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        if c in df.columns:
            s = pd.to_datetime(df[c], errors="coerce", utc=False)
            # to Python datetime for ODBC
            df[c] = s.dt.to_pydatetime()

    # Numbers: leave as strings/None; staging table is NVARCHAR and MERGE inserts into final
    return df


# -----------------------------
# SQL helpers (resilient)
# -----------------------------

def _augment_odbc_params(odbc: str) -> str:
    """Ensure retry/timeouts are present in the given odbc_connect string."""
    # add if missing; case-insensitive keys
    kv = {m.group(1).lower(): m.group(2) for m in re.finditer(r"([^;=]+)=([^;]*);?", odbc)}
    def add_if_missing(k, v): 
        if k.lower() not in kv:
            return f"{k}={v};"
        return ""
    suffix = ""
    suffix += add_if_missing("Encrypt", "yes")
    suffix += add_if_missing("TrustServerCertificate", "no")
    suffix += add_if_missing("Connection Timeout", "60")
    suffix += add_if_missing("LoginTimeout", "30")
    suffix += add_if_missing("ConnectRetryCount", "3")
    suffix += add_if_missing("ConnectRetryInterval", "5")
    suffix += add_if_missing("MARS_Connection", "Yes")
    return odbc + ("" if odbc.endswith(";") else ";") + suffix

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        odbc_connect = _augment_odbc_params(odbc_connect)
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    # pool_pre_ping helps recover dead connections automatically
    return create_engine(engine_url, fast_executemany=True, future=True, pool_pre_ping=True)

def qident(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"

def ensure_staging_table(engine: Engine, schema: str, stage_table: str):
    # Make a NVARCHAR(MAX)-only staging table to absorb any type issues
    cols_sql = ",\n".join(f"{qident(c)} NVARCHAR(MAX) NULL" for c in DEST_COLS)
    sql = f"""
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :tname
)
BEGIN
    EXEC('CREATE TABLE {qident(schema)}.{qident(stage_table)} (
{cols_sql}
    )');
END
"""
    with engine.begin() as conn:
        conn.execute(text(sql), {"schema": schema, "tname": stage_table})

def write_stage(engine: Engine, schema: str, stage_table: str, df: pd.DataFrame) -> int:
    """Chunked executemany with retries."""
    if df.empty:
        return 0

    # Convert DataFrame -> list of tuples (strings or None)
    rows: List[Tuple[Optional[str], ...]] = []
    for _, r in df.iterrows():
        rec = []
        for c in DEST_COLS:
            v = r[c]
            if pd.isna(v):
                rec.append(None)
            elif isinstance(v, (datetime, pd.Timestamp)):
                rec.append(v.strftime("%Y-%m-%d"))
            elif isinstance(v, (float, int)):
                rec.append(str(v))
            else:
                rec.append(str(v))
        rows.append(tuple(rec))

    insert_sql = f"INSERT INTO {qident(schema)}.{qident(stage_table)} ({', '.join(qident(c) for c in DEST_COLS)}) VALUES ({', '.join(['?']*len(DEST_COLS))})"

    total = 0
    n = len(rows)
    chunks = math.ceil(n / STAGE_CHUNK_ROWS)
    start = 0
    for i in range(chunks):
        batch = rows[start:start+STAGE_CHUNK_ROWS]
        start += STAGE_CHUNK_ROWS
        # retry each batch
        last_err = None
        for attempt in range(1, SQL_RETRIES + 1):
            try:
                raw = engine.raw_connection()
                try:
                    cur = raw.cursor()
                    cur.fast_executemany = True
                    cur.executemany(insert_sql, batch)
                    raw.commit()
                finally:
                    raw.close()
                total += len(batch)
                break
            except Exception as e:
                last_err = e
                if attempt == SQL_RETRIES:
                    raise
                print(f"  staging batch {i+1}/{chunks}: retry {attempt}/{SQL_RETRIES} after error: {e}")
                time.sleep(RETRY_SLEEP)
        if last_err:
            raise last_err
    return total

def merge_stage_into_final(engine: Engine, schema: str, stage_table: str, final_table: str):
    """
    Merge rows that don't already exist (skip duplicates). We assume the PK is (companies_house_registered_number, date)
    or similar on the final table. Since we don't know your exact PK columns, we use (company_id, date) if present,
    else (companies_house_registered_number, date). Adjust ON as needed.
    """
    # Pick join keys heuristically
    join_left = "company_id"
    if join_left not in DEST_COLS:
        join_left = "companies_house_registered_number"

    on_clause = f"t.{qident(join_left)} = s.{qident(join_left)} AND t.{qident('date')} = s.{qident('date')}"
    # Columns to insert (all)
    cols = ", ".join(qident(c) for c in DEST_COLS)
    src_cols = ", ".join(f"s.{qident(c)}" for c in DEST_COLS)

    sql = f"""
MERGE {qident(schema)}.{qident(final_table)} AS t
USING {qident(schema)}.{qident(stage_table)} AS s
ON ({on_clause})
WHEN NOT MATCHED THEN
    INSERT ({cols}) VALUES ({src_cols});
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

def clear_stage(engine: Engine, schema: str, stage_table: str):
    with engine.begin() as conn:
        conn.exec_driver_sql(f"TRUNCATE TABLE {qident(schema)}.{qident(stage_table)}")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (robust).")
    # Month selection
    ap.add_argument("--start-month", help="YYYY-MM inclusive.")
    ap.add_argument("--end-month", help="YYYY-MM inclusive.")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list.")
    ap.add_argument("--since-months", type=int, default=1)
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--staging-table", help="Override staging table name")
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

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    staging = args.staging_table or f"{args.target_table}_stage"
    try:
        ensure_staging_table(engine, args.schema, staging)
    except Exception as e:
        print(f"ERROR ensuring staging table: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # --- fetch & parse (with retries + body-length check)
        try:
            columns, rows_iter = fetch_month_zip_as_rows(ym, timeout=args.timeout)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        # Stream rows into DataFrame in moderate chunks to keep memory steady
        CHUNK = 200_000
        buf: List[List[str]] = []
        chunk_idx = 0
        try:
            for row in rows_iter:
                buf.append([("" if v is None else str(v)) for v in row])
                if len(buf) >= CHUNK:
                    chunk_idx += 1
                    df = pd.DataFrame(buf, columns=columns)
                    df = normalise_df(df, zip_url=candidate_urls_for_month(ym)[0])
                    # write to staging
                    write_stage(engine, args.schema, staging, df)
                    buf.clear()
            # tail
            if buf:
                df = pd.DataFrame(buf, columns=columns)
                df = normalise_df(df, zip_url=candidate_urls_for_month(ym)[0])
                write_stage(engine, args.schema, staging, df)
                buf.clear()
        except Exception as e:
            print(f"{ym}: staging SQL error: {e}")
            return 1

        # Merge into final (skip duplicates)
        try:
            merge_stage_into_final(engine, args.schema, staging, args.target_table)
            clear_stage(engine, args.schema, staging)
            wrote_any = True
            print(f"✅ {ym}: merged into {args.schema}.{args.target_table}")
        except Exception as e:
            print(f"{ym}: merge error: {e}")
            return 1

    if not wrote_any:
        print("Nothing ingested.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
