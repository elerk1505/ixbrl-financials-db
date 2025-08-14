#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest Companies House Accounts monthly/annual ZIPs into yearly_sqlites/<YEAR>.sqlite

Run a whole year:
  python ingest_monthly_to_sqlite.py --year 2025
  python ingest_monthly_to_sqlite.py --year 2024
  python ingest_monthly_to_sqlite.py --year 2009

Or a month range:
  python ingest_monthly_to_sqlite.py --start 2024-06 --end 2025-06

Notes:
- For 2008 & 2009, Companies House provides one annual archive:
    https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember2009.zip
    https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember2008.zip
- For 2010–2024, monthly ZIPs live under /archive/, e.g. .../archive/Accounts_Monthly_Data-June2024.zip
- For 2025+, the current months are on the main path (non-archive) with fallback to /archive/.
- Idempotent: UPSERT on (company_number, period_end) – re-runs won’t duplicate.
"""

import argparse
import calendar
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import httpx
import pandas as pd
import sqlite3
from stream_read_xbrl import stream_read_xbrl_zip

# -----------------------------
# Config
# -----------------------------
OUTPUT_DIR = Path("yearly_sqlites")
TABLE_NAME = "financials"
BATCH_ROWS = 200_000
HTTP_TIMEOUT = 180.0

MONTH_NAMES = {i: calendar.month_name[i] for i in range(1, 13)}  # 1->'January', ...

# -----------------------------
# SQLite helpers (UPSERT logic)
# -----------------------------

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # company_number
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)

    # standardise period_end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns and "period_end" not in df.columns:
            df = df.rename(columns={cand: "period_end"})
            break

    return df

def upsert_df_into_year_db(df: pd.DataFrame, db_path: Path, table: str = TABLE_NAME) -> None:
    """UPSERT a DataFrame into a per-year SQLite DB."""
    df = normalize_df(df)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        # Stage the chunk
        df.to_sql("_staging", con, if_exists="replace", index=False)
        cur = con.cursor()

        # Read target/staging columns
        tgt_cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{table}")')]
        stg_cols = [r[1] for r in cur.execute('PRAGMA table_info("_staging")')]
        stg_cols_set = set(stg_cols)

        # Create target table if missing (with all staging columns)
        if not tgt_cols:
            columns_formatted = ", ".join([f'"{c}"' for c in stg_cols]) or "dummy INTEGER"
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns_formatted})')
            tgt_cols = stg_cols

        # Add any missing columns
        for c in stg_cols:
            if c not in tgt_cols:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}"')

        # Prepare column list strings (avoid nested f-strings)
        col_list_sql = ", ".join([f'"{c}"' for c in stg_cols])

        # Unique index for upsert when we have both keys
        if {"company_number", "period_end"}.issubset(stg_cols_set):
            cur.execute(
                f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_company_period ON "{table}"(company_number, period_end)'
            )
            cur.execute(f'INSERT OR REPLACE INTO "{table}" ({col_list_sql}) SELECT {col_list_sql} FROM "_staging"')
        else:
            cur.execute(f'INSERT INTO "{table}" ({col_list_sql}) SELECT {col_list_sql} FROM "_staging"')

        # Drop staging and add helpful indexes
        cur.execute('DROP TABLE IF EXISTS "_staging"')
        if "company_number" in stg_cols_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_company ON "{table}"(company_number)')
        if "period_end" in stg_cols_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_period ON "{table}"(period_end)')
        con.commit()

# -----------------------------
# Month iteration + URL logic
# -----------------------------

@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def to_display_name(self) -> str:
        return f"{MONTH_NAMES[self.month]}{self.year}"

    def next(self) -> "YearMonth":
        y, m = self.year, self.month + 1
        if m == 13:
            y, m = y + 1, 1
        return YearMonth(y, m)

def iter_months_inclusive(start: YearMonth, end: YearMonth) -> Iterable[YearMonth]:
    cur = start
    while True:
        yield cur
        if cur.year == end.year and cur.month == end.month:
            break
        cur = cur.next()

def monthly_urls(ym: YearMonth) -> List[str]:
    disp = ym.to_display_name()  # e.g., "June2025"
    # Try main first, then archive
    return [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{disp}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{disp}.zip",
    ]

def annual_archive_url_2008_2009(year: int) -> str:
    # e.g. .../archive/Accounts_Monthly_Data-JanuaryToDecember2009.zip
    return f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip"

# -----------------------------
# Download + ingest
# -----------------------------

def process_zip_from_urls(urls: List[str], batch_rows: int = BATCH_ROWS, timeout: float = HTTP_TIMEOUT) -> Tuple[int, set]:
    """
    Try a list of candidate URLs (first that works wins). Stream, parse, and upsert.
    Returns (rows_ingested, years_touched)
    """
    last_err: Optional[Exception] = None
    for url in urls:
        print(f"📦 Fetching → {url}")
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                buffer = []
                years_touched = set()
                rows_ingested = 0
                with stream_read_xbrl_zip(r.iter_bytes()) as (columns, row_iter):
                    for row in row_iter:
                        buffer.append([("" if v is None else str(v)) for v in row])
                        if len(buffer) >= batch_rows:
                            rows_ingested += _flush_batch(buffer, columns, years_touched)
                            buffer.clear()
                    # flush remainder
                    if buffer:
                        rows_ingested += _flush_batch(buffer, columns, years_touched)
                        buffer.clear()
                print(f"✅ Upserted {rows_ingested:,} rows across {len(years_touched)} year DB(s)")
                return rows_ingested, years_touched
        except httpx.HTTPStatusError as e:
            last_err = e
            print(f"  ↪️ HTTP {e.response.status_code} for {url}, trying fallback…")
            continue
        except Exception as e:
            last_err = e
            print(f"  ❌ Error for {url}: {e}, trying fallback…")
            continue
    print(f"❌ Could not fetch any URL. Last error: {last_err}")
    return 0, set()

def _flush_batch(buffer: List[List[str]], columns: List[str], years_touched: set) -> int:
    df = pd.DataFrame(buffer, columns=columns)
    # Determine target year(s) from balance_sheet_date
    df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
    df = df.dropna(subset=["balance_sheet_date"])
    if df.empty:
        return 0
    df["year"] = df["balance_sheet_date"].dt.year
    total = 0
    for year, df_year in df.groupby("year"):
        out_db = OUTPUT_DIR / f"{int(year)}.sqlite"
        upsert_df_into_year_db(df_year, out_db)
        years_touched.add(int(year))
        total += len(df_year)
    return total

# -----------------------------
# CLI
# -----------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Ingest Companies House Accounts into yearly SQLite DBs")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--year", type=int, help="Ingest a whole year (e.g., 2025, 2024, … 2008)")
    mode.add_argument("--start", help="Start month in YYYY-MM (e.g., 2023-01)")
    ap.add_argument("--end", help="End month in YYYY-MM (inclusive, e.g., 2025-06)")
    ap.add_argument("--batch-rows", type=int, default=BATCH_ROWS, help="Rows per batch to upsert (default 200k)")
    ap.add_argument("--timeout", type=float, default=HTTP_TIMEOUT, help="HTTP timeout seconds (default 180)")
    return ap.parse_args()

def ingest_year(year: int, batch_rows: int, timeout: float) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 2008/2009: one annual ZIP
    if year in (2008, 2009):
        url = annual_archive_url_2008_2009(year)
        print(f"⏳ Ingesting annual archive for {year}")
        process_zip_from_urls([url], batch_rows=batch_rows, timeout=timeout)
        return

    # 2010–2024: monthly ZIPs in /archive/
    if 2010 <= year <= 2024:
        for m in range(1, 13):
            ym = YearMonth(year, m)
            urls = [f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{ym.to_display_name()}.zip"]
            print(f"⏳ Ingesting {ym}")
            process_zip_from_urls(urls, batch_rows=batch_rows, timeout=timeout)
        return

    # 2025+: monthly ZIPs: try main path then archive
    if year >= 2025:
        # Attempt months up to current month if it's the current year; otherwise all 12
        last_month = 12
        now = datetime.utcnow()
        if year == now.year:
            last_month = now.month
        for m in range(1, last_month + 1):
            ym = YearMonth(year, m)
            urls = [
                f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{ym.to_display_name()}.zip",
                f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{ym.to_display_name()}.zip",
            ]
            print(f"⏳ Ingesting {ym}")
            process_zip_from_urls(urls, batch_rows=batch_rows, timeout=timeout)
        return

    # Fallback for any other year (treat like 2010–2024)
    for m in range(1, 13):
        ym = YearMonth(year, m)
        urls = [f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{ym.to_display_name()}.zip"]
        print(f"⏳ Ingesting {ym}")
        process_zip_from_urls(urls, batch_rows=batch_rows, timeout=timeout)

def main():
    args = parse_args()
    if args.year:
        ingest_year(args.year, batch_rows=args.batch_rows, timeout=args.timeout)
        print("✅ Done.")
        return 0

    # month range mode
    if not args.end:
        print("When using --start, you must also provide --end (e.g., --start 2024-06 --end 2025-06)")
        return 2

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m")
        end_dt = datetime.strptime(args.end, "%Y-%m")
    except ValueError:
        print("Start/End must be in YYYY-MM format, e.g., --start 2023-01 --end 2025-06")
        return 2
    if (end_dt.year, end_dt.month) < (start_dt.year, start_dt.month):
        print("End month must be >= start month")
        return 2

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    months_done = 0
    for ym in iter_months_inclusive(YearMonth(start_dt.year, start_dt.month), YearMonth(end_dt.year, end_dt.month)):
        rows, _ = process_zip_from_urls(
            monthly_urls(ym), batch_rows=args.batch_rows, timeout=args.timeout
        )
        total_rows += rows
        months_done += 1

    print(f"✅ Finished. Months: {months_done}, total rows upserted: {total_rows:,}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
