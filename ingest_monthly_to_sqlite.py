#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

OUTPUT_DIR = Path("yearly_sqlites")
TABLE = "financials"
BATCH_ROWS = 200_000
HTTP_TIMEOUT = 180.0
MONTH_NAMES = {i: calendar.month_name[i] for i in range(1, 13)}

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns and "period_end" not in df.columns:
            df = df.rename(columns={cand: "period_end"})
            break
    return df

def target_db_for_date(dt: pd.Timestamp) -> Path:
    half = 1 if dt.month <= 6 else 2
    return OUTPUT_DIR / f"{dt.year}_{half}.sqlite"

def upsert_df(df: pd.DataFrame, db_path: Path, table: str = TABLE) -> None:
    df = normalize_df(df)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        df.to_sql("_staging", con, if_exists="replace", index=False)
        cur = con.cursor()
        tgt_cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{table}")')]
        stg_cols = [r[1] for r in cur.execute('PRAGMA table_info("_staging")')]
        stg_set = set(stg_cols)
        if not tgt_cols:
            cols_sql = ", ".join([f'"{c}"' for c in stg_cols]) or "dummy INTEGER"
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
            tgt_cols = stg_cols
        for c in stg_cols:
            if c not in tgt_cols:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}"')
        col_list = ", ".join([f'"{c}"' for c in stg_cols])
        if {"company_number", "period_end"}.issubset(stg_set):
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_company_period ON "{table}"(company_number, period_end)')
            cur.execute(f'INSERT OR REPLACE INTO "{table}" ({col_list}) SELECT {col_list} FROM "_staging"')
        else:
            cur.execute(f'INSERT INTO "{table}" ({col_list}) SELECT {col_list} FROM "_staging"')
        cur.execute('DROP TABLE IF EXISTS "_staging"')
        if "company_number" in stg_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_company ON "{table}"(company_number)')
        if "period_end" in stg_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_period ON "{table}"(period_end)')
        con.commit()

@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int
    def __str__(self) -> str: return f"{self.year:04d}-{self.month:02d}"
    def to_display_name(self) -> str: return f"{MONTH_NAMES[self.month]}{self.year}"
    def next(self) -> "YearMonth":
        y, m = self.year, self.month + 1
        if m == 13: y, m = y + 1, 1
        return YearMonth(y, m)

def iter_months_inclusive(start: YearMonth, end: YearMonth) -> Iterable[YearMonth]:
    cur = start
    while True:
        yield cur
        if (cur.year, cur.month) == (end.year, end.month):
            break
        cur = cur.next()

def monthly_urls(ym: YearMonth) -> List[str]:
    disp = ym.to_display_name()
    return [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{disp}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{disp}.zip",
    ]

def annual_archive_url_2008_2009(year: int) -> str:
    return f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip"

def _flush_batch(buffer: List[List[str]], columns: List[str], years_touched: set) -> int:
    df = pd.DataFrame(buffer, columns=columns)
    df["balance_sheet_date"] = pd.to_datetime(df.get("balance_sheet_date"), errors="coerce")
    df = df.dropna(subset=["balance_sheet_date"])
    if df.empty:
        return 0
    total = 0
    # route by half-year
    df["year"] = df["balance_sheet_date"].dt.year
    df["half"] = (df["balance_sheet_date"].dt.month <= 6).map({True: 1, False: 2})
    for (year, half), part in df.groupby(["year", "half"]):
        out_db = OUTPUT_DIR / f"{int(year)}_{int(half)}.sqlite"
        upsert_df(part, out_db)
        years_touched.add((int(year), int(half)))
        total += len(part)
    return total

def process_zip_from_urls(urls: List[str], batch_rows: int = BATCH_ROWS, timeout: float = HTTP_TIMEOUT) -> Tuple[int, set]:
    last_err: Optional[Exception] = None
    for url in urls:
        print(f"📦 Fetching → {url}")
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()
                buffer: List[List[str]] = []
                years_touched: set = set()
                rows_ingested = 0
                with stream_read_xbrl_zip(r.iter_bytes()) as (columns, row_iter):
                    for row in row_iter:
                        buffer.append([("" if v is None else str(v)) for v in row])
                        if len(buffer) >= batch_rows:
                            rows_ingested += _flush_batch(buffer, columns, years_touched)
                            buffer.clear()
                    if buffer:
                        rows_ingested += _flush_batch(buffer, columns, years_touched)
                        buffer.clear()
                print(f"✅ Upserted {rows_ingested:,} rows across {len(years_touched)} half-year DB(s)")
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

def parse_args():
    ap = argparse.ArgumentParser(description="Ingest Companies House Accounts into half-year SQLite DBs")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--year", type=int, help="Ingest a whole year (e.g., 2025, 2024, … 2008)")
    mode.add_argument("--start", help="Start month in YYYY-MM (e.g., 2023-01)")
    ap.add_argument("--end", help="End month in YYYY-MM (inclusive, e.g., 2025-06)")
    ap.add_argument("--batch-rows", type=int, default=BATCH_ROWS)
    ap.add_argument("--timeout", type=float, default=HTTP_TIMEOUT)
    return ap.parse_args()

def ingest_year(year: int, batch_rows: int, timeout: float) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if year in (2008, 2009):
        url = annual_archive_url_2008_2009(year)
        print(f"⏳ Ingesting annual archive for {year}")
        process_zip_from_urls([url], batch_rows=batch_rows, timeout=timeout)
        return
    if 2010 <= year <= 2024:
        for m in range(1, 13):
            ym = YearMonth(year, m)
            urls = [f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{ym.to_display_name()}.zip"]
            print(f"⏳ Ingesting {ym}")
            process_zip_from_urls(urls, batch_rows=batch_rows, timeout=timeout)
        return
    if year >= 2025:
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
    # Fallback
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
    if not args.end:
        print("When using --start, you must also provide --end (e.g., --start 2024-06 --end 2025-06)")
        return 2
    try:
        start_dt = datetime.strptime(args.start, "%Y-%m")
        end_dt = datetime.strptime(args.end, "%Y-%m")
    except ValueError:
        print("Start/End must be in YYYY-MM format")
        return 2
    if (end_dt.year, end_dt.month) < (start_dt.year, start_dt.month):
        print("End month must be >= start month")
        return 2

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    months_done = 0
    cur = YearMonth(start_dt.year, start_dt.month)
    end = YearMonth(end_dt.year, end_dt.month)
    while True:
        rows, _ = process_zip_from_urls(monthly_urls(cur), batch_rows=args.batch_rows, timeout=args.timeout)
        total_rows += rows
        months_done += 1
        if (cur.year, cur.month) == (end.year, end.month):
            break
        cur = cur.next()
    print(f"✅ Finished. Months: {months_done}, total rows upserted: {total_rows:,}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
