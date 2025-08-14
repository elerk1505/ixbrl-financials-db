#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Companies House company profiles for a growing list of company numbers.

Features
- Reads company numbers from a CSV that you keep updating (re-scans each run).
- Caches results in SQLite (resume-safe).
- Concurrency with aiohttp; handles 429 (Retry-After) and transient errors.
- Only fetches missing rows by default; use --force to refresh all or --since-days to refresh stale rows.
- Simple CLI for convenience.

Usage:
  python fetch_company_metadata.py --api-key <YOUR_CH_API_KEY> \
      --csv company_numbers.csv \
      --db company_metadata.sqlite \
      --concurrency 8 \
      --timeout 20

CSV requirements:
  Column name "company_number" (preferred) OR "companies_house_registered_number".
  You can override with --column.

Database schema (table company_profiles):
  company_number TEXT PRIMARY KEY
  fetched_at TEXT            # ISO timestamp when fetched
  http_status INTEGER        # HTTP status code
  etag TEXT                  # Response ETag header, if any
  payload_json TEXT          # Raw JSON of the profile response
  error TEXT                 # Error summary, if any
"""
import argparse
import asyncio
import aiohttp
import csv
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Iterable, List

DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY = 8

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

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(CREATE_SQL)
    for ddl in CREATE_INDEXES:
        conn.execute(ddl)
    conn.commit()
    return conn

def csv_company_numbers(csv_path: str, column: Optional[str] = None) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    candidates = ["company_number", "companies_house_registered_number"]
    if column:
        candidates = [column] + [c for c in candidates if c != column]

    numbers = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Resolve column name
        col = None
        header_lc = {h.lower(): h for h in reader.fieldnames or []}
        for cand in candidates:
            if cand in header_lc:
                col = header_lc[cand]
                break
        if not col:
            raise ValueError(f"CSV must include one of columns {candidates}; found: {reader.fieldnames}")

        for row in reader:
            val = (row.get(col) or "").strip()
            if not val:
                continue
            val = val.replace(" ", "")
            numbers.append(val)
    # de-dup while keeping order
    seen = set()
    uniq = []
    for n in numbers:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq

def existing_company_numbers(conn: sqlite3.Connection) -> set:
    cur = conn.execute("SELECT company_number FROM company_profiles")
    return {r[0] for r in cur.fetchall()}

def needs_refresh(conn: sqlite3.Connection, since_days: Optional[int]) -> set:
    if since_days is None:
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
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
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

async def worker(name: int, queue: asyncio.Queue, session: aiohttp.ClientSession, api_key: str, timeout: int, conn: sqlite3.Connection):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        company_number = item
        res = await fetch_profile(session, api_key, company_number, timeout)
        upsert_profile(conn, company_number, res)
        queue.task_done()

async def run_pipeline(api_key: str, csv_path: str, db_path: str, column: Optional[str], concurrency: int, timeout: int, force: bool, since_days: Optional[int]):
    conn = init_db(db_path)
    # Read desired set from CSV
    target_numbers = csv_company_numbers(csv_path, column=column)

    # Decide which to fetch
    existing = existing_company_numbers(conn)
    stale = needs_refresh(conn, since_days) if since_days else set()

    if force:
        to_fetch = target_numbers
    else:
        to_fetch = [n for n in target_numbers if (n not in existing) or (n in stale)]

    total = len(to_fetch)
    print(f"Total in CSV: {len(target_numbers)} | Already in DB: {len(existing)} | To fetch now: {total}")

    if total == 0:
        conn.commit()
        conn.close()
        return

    import asyncio
    queue: asyncio.Queue = asyncio.Queue()
    for n in to_fetch:
        queue.put_nowait(n)
    for _ in range(concurrency):
        queue.put_nowait(None)

    timeout_obj = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        workers = [
            asyncio.create_task(worker(i, queue, session, api_key, timeout, conn))
            for i in range(concurrency)
        ]
        # Simple progress output
        while any(not w.done() for w in workers):
            await asyncio.sleep(0.5)

        await queue.join()
        for w in workers:
            await w

    conn.commit()
    conn.close()
    print("Done.")

def main():
    p = argparse.ArgumentParser(description="Fetch Companies House company profiles into SQLite.")
    p.add_argument("--api-key", required=True, help="Companies House API key")
    p.add_argument("--csv", required=True, help="Path to CSV containing company numbers")
    p.add_argument("--db", default="company_metadata.sqlite", help="SQLite DB path (will be created if missing)")
    p.add_argument("--column", default=None, help="Column name in CSV; default tries common names")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent requests")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Per-request socket timeout (seconds)")
    p.add_argument("--force", action="store_true", help="Force refetch of all company numbers in CSV")
    p.add_argument("--since-days", type=int, default=None, help="Refetch rows older than N days")
    args = p.parse_args()

    asyncio.run(run_pipeline(
        api_key=args.api_key,
        csv_path=args.csv,
        db_path=args.db,
        column=args.column,
        concurrency=args.concurrency,
        timeout=args.timeout,
        force=args.force,
        since_days=args.since_days,
    ))

if __name__ == "__main__":
    main()
