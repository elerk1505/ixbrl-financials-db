#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest Companies House Monthly Accounts ZIPs into yearly_sqlites/<YEAR>.sqlite

Usage (examples):
  # Your full range from Jan 2023 to Jun 2025 (inclusive)
  python ingest_monthly_to_sqlite.py --start 2023-01 --end 2025-06

  # Just 2024-06 to 2025-06 (inclusive)
  python ingest_monthly_to_sqlite.py --start 2024-06 --end 2025-06

Notes:
- Tries https://download.companieshouse.gov.uk/Accounts_Monthly_Data-<MonthName><YYYY>.zip
  and falls back to https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-<MonthName><YYYY>.zip
- Writes per-year SQLite DBs under yearly_sqlites/<YEAR>.sqlite, table "financials".
"""

import argparse
import calendar
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple

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
HTTP_TIMEOUT = 120.0

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
# Month iteration + download
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


def fetch_and_ingest_month(ym: YearMonth, batch_rows: int = BATCH_ROWS, timeout: float = HTTP_TIMEOUT) -> Tuple[int, int]:
    """
    Downloads one month's ZIP (trying main then archive), parses rows in batches,
    and upserts them into yearly_sqlites/<YEAR>.sqlite.

    Returns: (rows_ingested, years_touched)
    """
    rows_ingested = 0
    years_touched = set()
    last_err = None

    for url in monthly_urls(ym):
        print(f"📦 Fetching {ym} → {url}")
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                # Stream rows → batch → DataFrame → upsert
                buffer = []
                columns = None
                with stream_read_xbrl_zip(r.iter_bytes()) as (cols, row_iter):
                    columns = cols
                    for row in row_iter:
                        # normalize to strings for stability; keep None if you prefer
                        buffer.append([("" if v is None else str(v)) for v in row])
                        if len(buffer) >= batch_rows:
                            df = pd.DataFrame(buffer, columns=columns)
                            # determine years from balance_sheet_date
                            df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
                            df = df.dropna(subset=["balance_sheet_date"])
                            if df.empty:
                                buffer.clear()
                                continue
                            df["year"] = df["balance_sheet_date"].dt.year
                            for year, df_year in df.groupby("year"):
                                out_db = OUTPUT_DIR / f"{int(year)}.sqlite"
                                upsert_df_into_year_db(df_year, out_db)
                                years_touched.add(int(year))
                            rows_ingested += len(df)
                            buffer.clear()

                    # flush remainder
                    if buffer:
                        df = pd.DataFrame(buffer, columns=columns)
                        df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
                        df = df.dropna(subset=["balance_sheet_date"])
                        if not df.empty:
                            df["year"] = df["balance_sheet_date"].dt.year
                            for year, df_year in df.groupby("year"):
                                out_db = OUTPUT_DIR / f"{int(year)}.sqlite"
                                upsert_df_into_year_db(df_year, out_db)
                                years_touched.add(int(year))
                            rows_ingested += len(df)
                        buffer.clear()
                print(f"✅ {ym}: upserted {rows_ingested:,} rows across {len(years_touched)} year DB(s)")
                return rows_ingested, len(years_touched)
        except httpx.HTTPStatusError as e:
            # try next URL (maybe in archive)
            last_err = e
            print(f"  ↪️ HTTP {e.response.status_code} for {url}, trying fallback...")
            continue
        except Exception as e:
            last_err = e
            print(f"  ❌ Error for {url}: {e}")
            continue

    print(f"❌ Failed to fetch {ym} from main or archive. Last error: {last_err}")
    return 0, 0


# -----------------------------
# CLI
# -----------------------------


def parse_args():
    ap = argparse.ArgumentParser(description="Ingest Companies House Monthly Accounts into yearly SQLite DBs")
    ap.add_argument("--start", required=True, help="Start month in YYYY-MM (e.g., 2023-01)")
    ap.add_argument("--end", required=True, help="End month in YYYY-MM (inclusive, e.g., 2025-06)")
    ap.add_argument("--batch-rows", type=int, default=BATCH_ROWS, help="Rows per batch to upsert (default 200k)")
    ap.add_argument("--timeout", type=float, default=HTTP_TIMEOUT, help="HTTP timeout seconds (default 120)")
    return ap.parse_args()


def main():
    args = parse_args()

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
        rows, _ = fetch_and_ingest_month(ym, batch_rows=args.batch_rows, timeout=args.timeout)
        total_rows += rows
        months_done += 1

    print(f"✅ Finished. Months: {months_done}, total rows upserted: {total_rows:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
