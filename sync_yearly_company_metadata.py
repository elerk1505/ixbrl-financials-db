#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync metadata for companies listed in yearly_sqlites/*.sqlite (table: financials)

What it does:
1) Scans per-year SQLite files under --input-dir (default: yearly_sqlites), e.g., 2020.sqlite … 2025.sqlite
2) Collects DISTINCT company numbers from the 'financials' table (prefers 'company_number', falls back to 'companies_house_registered_number')
3) Writes a deduped list to data/company_numbers.csv
4) Fetches Companies House /company/{number} profiles for those IDs
5) Stores results in data/company_metadata.sqlite, one row per company
6) Supports --max-per-run to limit how many NEW/stale companies to fetch this run

Usage (local):
  export CH_API_KEY=YOUR_COMPANIES_HOUSE_API_KEY
  python sync_yearly_company_metadata.py --input-dir yearly_sqlites --max-per-run 50000

You can re-run anytime; it resumes automatically and only fetches missing rows (or use --since-days / --force).

Notes:
- CH profile endpoint uses Basic auth with API key as username and empty password.
- Script uses polite concurrency, retries, and respects Retry-After on 429.
"""

import argparse
import asyncio
import csv
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import aiohttp

DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY = 8

# SQLite (metadata DB)
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS company_profiles (
    company_number TEXT PRIMARY KEY,
    fetched_at TEXT,
    http_status INTEGER,
    etag TEXT,
    payload_json TEXT,
    error TEXT
);
"""
CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_company_profiles_status ON company_profiles(http_status)",
    "CREATE INDEX IF NOT EXISTS idx_company_profiles_fetched_at ON company_profiles(fetched_at)"
]

def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(CREATE_SQL)
    for ddl in CREATE_INDEXES:
        conn.execute(ddl)
    conn.commit()
    return conn

# ---------- NEW: read company numbers from yearly_sqlites/*.sqlite ----------

def collect_company_numbers_from_sqlites(input_dir: Path, table: str = "financials") -> List[str]:
    """
    Open each *.sqlite under input_dir and SELECT DISTINCT company numbers.
    Prefers 'company_number'; falls back to 'companies_house_registered_number' if needed.
    Normalizes: strip spaces, zfill to 8 digits.
    """
    if not input_dir.exists():
        print(f"WARNING: input dir not found: {input_dir}")
        return []

    numbers_set = set()
    db_files = sorted(input_dir.glob("*.sqlite"))
    if not db_files:
        print(f"WARNING: no .sqlite files found in {input_dir}")
        return []

    for db in db_files:
        try:
            with sqlite3.connect(db) as con:
                cur = con.cursor()
                # Detect available ID column
                cols = {r[1].lower(): r[1] for r in cur.execute(f'PRAGMA table_info("{table}")')}
                id_col = None
                for cand in ("company_number", "companies_house_registered_number"):
                    if cand in cols:
                        id_col = cols[cand]
                        break
                if not id_col:
                    print(f"MISS (no ID column) {db}::{table}")
                    continue

                # Use DISTINCT to minimize memory
                for (raw,) in cur.execute(f'SELECT DISTINCT "{id_col}" FROM "{table}"'):
                    if raw is None:
                        continue
                    v = str(raw).strip().replace(" ", "")
                    if not v:
                        continue
                    # normalize to 8-digit numeric-ish IDs if possible
                    if v.isdigit():
                        v = v.zfill(8)
                    numbers_set.add(v)
                print(f"OK   (+{len(numbers_set):,} total ids) {db}")
        except Exception as e:
            print(f"SKIP (error reading {db}): {e}")

    # deterministic order
    return sorted(numbers_set)

# ---------------------------------------------------------------------------

def write_company_numbers_csv(numbers: List[str], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_number"])
        for n in numbers:
            w.writerow([n])
    print(f"Wrote {len(numbers):,} unique IDs -> {out_csv}")

def existing_company_numbers(conn: sqlite3.Connection) -> set:
    cur = conn.execute("SELECT company_number FROM company_profiles")
    return {r[0] for r in cur.fetchall()}

def stale_company_numbers(conn: sqlite3.Connection, since_days: Optional[int]) -> set:
    if not since_days:
        return set()
    cur = conn.execute(
        "SELECT company_number FROM company_profiles WHERE fetched_at IS NOT NULL AND datetime(fetched_at) <= datetime('now', ?)",
        (f"-{since_days} days",)
    )
    return {r[0] for r in cur.fetchall()}

async def fetch_profile(session: aiohttp.ClientSession, api_key: str, company_number: str, timeout: int) -> Dict[str, Any]:
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    for attempt in range(8):
        try:
            async with session.get(url, timeout=timeout, auth=aiohttp.BasicAuth(api_key, "")) as resp:
                status = resp.status
                etag = resp.headers.get("ETag")
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    data = {"raw": text}

                if status == 200:
                    return {"status": status, "etag": etag, "json": data, "error": None}
                elif status in (404, 410):
                    return {"status": status, "etag": etag, "json": data, "error": f"Not found ({status})"}
                elif status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                elif 500 <= status < 600:
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    return {"status": status, "etag": etag, "json": data, "error": f"HTTP {status}"}
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(min(60, 1.5 ** attempt))
    return {"status": None, "etag": None, "json": None, "error": f"Request failed after retries for {company_number}"}

def upsert_profile(conn: sqlite3.Connection, company_number: str, result: Dict[str, Any]) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(result.get("json"))
    conn.execute(
        """
        INSERT INTO company_profiles(company_number, fetched_at, http_status, etag, payload_json, error)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(company_number) DO UPDATE SET
            fetched_at=excluded.fetched_at,
            http_status=excluded.http_status,
            etag=excluded.etag,
            payload_json=excluded.payload_json,
            error=excluded.error
        """,
        (company_number, fetched_at, result.get("status"), result.get("etag"), payload_json, result.get("error")),
    )

async def worker(queue: asyncio.Queue, session: aiohttp.ClientSession, api_key: str, timeout: int, conn: sqlite3.Connection):
    while True:
        company_number = await queue.get()
        if company_number is None:
            queue.task_done()
            return
        res = await fetch_profile(session, api_key, company_number, timeout)
        upsert_profile(conn, company_number, res)
        queue.task_done()

async def fetch_many(api_key: str, numbers: List[str], db_path: Path, concurrency: int, timeout: int):
    conn = init_db(db_path)
    queue: asyncio.Queue = asyncio.Queue()

    for n in numbers:
        queue.put_nowait(n)
    for _ in range(concurrency):
        queue.put_nowait(None)

    timeout_obj = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        tasks = [asyncio.create_task(worker(queue, session, api_key, timeout, conn)) for _ in range(concurrency)]
        await queue.join()
        for t in tasks:
            await t
    conn.commit()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Sync Companies House metadata from yearly SQLites.")
    parser.add_argument("--input-dir", default="yearly_sqlites", help="Folder containing per-year SQLite files (e.g., 2020.sqlite)")
    parser.add_argument("--table", default="financials", help="Table name in each yearly SQLite file")
    parser.add_argument("--out-csv", default="data/company_numbers.csv", help="Output CSV of deduped company numbers")
    parser.add_argument("--db", default="data/company_metadata.sqlite", help="SQLite DB path for cached profiles")
    parser.add_argument("--max-per-run", type=int, default=None, help="Limit how many NEW/stale companies to fetch this run")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--since-days", type=int, default=None, help="Refresh rows older than N days (also counts as NEW)")
    parser.add_argument("--force", action="store_true", help="Refetch all company numbers regardless of cache")
    args = parser.parse_args()

    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        raise SystemExit("CH_API_KEY env var is required")

    input_dir = Path(args.input_dir)
    out_csv = Path(args.out_csv)
    db_path = Path(args.db)

    # 1) Collect & write company_numbers.csv (from yearly_sqlites/*.sqlite)
    numbers = collect_company_numbers_from_sqlites(input_dir=input_dir, table=args.table)
    if not numbers:
        print("No company IDs found in yearly_sqlites.")
    write_company_numbers_csv(numbers, out_csv)

    # 2) Decide which numbers to fetch
    conn = init_db(db_path)
    existing = existing_company_numbers(conn)
    stale = stale_company_numbers(conn, args.since_days) if args.since_days else set()

    if args.force:
        to_fetch = numbers
    else:
        to_fetch = [n for n in numbers if (n not in existing) or (n in stale)]

    if args.max_per_run is not None:
        to_fetch = to_fetch[: args.max_per_run]

    print(f"Totals — in yearly SQLites: {len(numbers):,} | already in DB: {len(existing):,} | stale: {len(stale):,} | fetching now: {len(to_fetch):,}")
    conn.close()

    if to_fetch:
        asyncio.run(fetch_many(api_key, to_fetch, db_path, args.concurrency, args.timeout))
        print("Fetch complete.")
    else:
        print("Nothing to fetch this run.")

if __name__ == "__main__":
    main()
