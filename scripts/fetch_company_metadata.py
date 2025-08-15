#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Companies House company profiles and cache them in SQLite.

Modes
-----
1) CSV-FREE (recommended):
   - Omit --csv. The script will read company numbers from the
     `company_numbers` table inside --db (data/company_metadata.sqlite).

2) Legacy CSV mode (optional):
   - Provide --csv with a CSV containing a "company_number" (or
     "companies_house_registered_number") column.

Behavior
--------
- Resume-safe caching in table `company_profiles`.
- Only fetches missing rows by default.
- Use --since-days N to refresh stale rows (rows with fetched_at <= now- N days).
- Use --force to refetch all.
- Handles 429 with Retry-After, plus transient errors, concurrent requests.

Schema
------
company_profiles (
    company_number TEXT PRIMARY KEY,
    fetched_at     TEXT,
    http_status    INTEGER,
    etag           TEXT,
    payload_json   TEXT,
    error          TEXT
)

Optionally (maintained by your other script):
company_numbers (
    company_number TEXT PRIMARY KEY
)
"""
from __future__ import annotations

import argparse
import asyncio
import aiohttp
import csv
import json
import os
import sqlite3
from pathlib import Path
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

# ------------------------
# DB helpers
# ------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    def _open_ok() -> sqlite3.Connection:
        conn = sqlite3.connect(str(db_path))
        # fast fail if not a real DB
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn
    try:
        conn = _open_ok()
    except sqlite3.DatabaseError:
        # Not a real SQLite (e.g., LFS pointer) → recreate
        try:
            os.remove(db_path)
        except FileNotFoundError:
            pass
        conn = _open_ok()

    conn.execute(CREATE_SQL)
    for ddl in CREATE_INDEXES:
        conn.execute(ddl)
    conn.commit()
    return conn

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (name,)
    )
    return cur.fetchone() is not None

# ------------------------
# Load target company IDs
# ------------------------

def csv_company_numbers(csv_path: str, column: Optional[str] = None) -> List[str]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    candidates = ["company_number", "companies_house_registered_number"]
    if column:
        candidates = [column] + [c for c in candidates if c != column]

    numbers: List[str] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Resolve column name (case-insensitive)
        header_lc = {h.lower(): h for h in (reader.fieldnames or [])}
        col = None
        for cand in candidates:
            if cand in header_lc:
                col = header_lc[cand]
                break
        if not col:
            raise ValueError(f"CSV must include one of columns {candidates}; found: {reader.fieldnames}")

        for row in reader:
            val = (row.get(col) or "").strip().replace(" ", "")
            if val:
                numbers.append(val)

    # De-dup preserving order
    seen, uniq = set(), []
    for n in numbers:
        if n not in seen:
            uniq.append(n); seen.add(n)
    return uniq

def db_company_numbers_for_fetch(conn: sqlite3.Connection) -> List[str]:
    """
    Read company IDs from company_numbers table (if present).
    Returns a de-duplicated list of strings.
    """
    if not table_exists(conn, "company_numbers"):
        return []
    try:
        rows = conn.execute("SELECT company_number FROM company_numbers").fetchall()
    except sqlite3.DatabaseError:
        return []
    out: List[str] = []
    seen = set()
    for (num,) in rows:
        n = ("" if num is None else str(num)).replace(" ", "")
        if n and n not in seen:
            out.append(n); seen.add(n)
    return out

# ------------------------
# Selection logic
# ------------------------

def existing_company_numbers(conn: sqlite3.Connection) -> set:
    cur = conn.execute("SELECT company_number FROM company_profiles")
    return {r[0] for r in cur.fetchall()}

def needs_refresh(conn: sqlite3.Connection, since_days: Optional[int]) -> set:
    if since_days is None:
        return set()
    cur = conn.execute(
        "SELECT company_number FROM company_profiles "
        "WHERE fetched_at IS NOT NULL AND datetime(fetched_at) <= datetime('now', ?)",
        (f"-{since_days} days",)
    )
    return {r[0] for r in cur.fetchall()}

# ------------------------
# HTTP fetch
# ------------------------

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
                    delay = float(retry_after) if (retry_after and retry_after.isdigit()) else (2 ** attempt)
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

# ------------------------
# Workers
# ------------------------

async def worker(name: int, queue: asyncio.Queue, session: aiohttp.ClientSession,
                 api_key: str, timeout: int, conn: sqlite3.Connection, db_lock: asyncio.Lock):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            return
        company_number = item
        res = await fetch_profile(session, api_key, company_number, timeout)
        # serialize DB writes to avoid interleaving on the same connection
        async with db_lock:
            upsert_profile(conn, company_number, res)
        queue.task_done()

# ------------------------
# Pipeline
# ------------------------

async def run_pipeline(api_key: str,
                      db_path: str,
                      csv_path: Optional[str],
                      column: Optional[str],
                      concurrency: int,
                      timeout: int,
                      force: bool,
                      since_days: Optional[int]):
    conn = init_db(Path(db_path))

    # Decide target set (CSV -> list OR DB->company_numbers)
    if csv_path:
        target_numbers = csv_company_numbers(csv_path, column=column)
        source_label = f"CSV:{csv_path}"
    else:
        target_numbers = db_company_numbers_for_fetch(conn)
        source_label = "DB:company_numbers"

    existing = existing_company_numbers(conn)
    stale = needs_refresh(conn, since_days) if since_days else set()

    if force:
        to_fetch = target_numbers
    else:
        to_fetch = [n for n in target_numbers if (n not in existing) or (n in stale)]

    print(f"Source {source_label} → {len(target_numbers):,} IDs | "
          f"already cached: {len(existing):,} | stale: {len(stale):,} | fetching now: {len(to_fetch):,}")

    if not to_fetch:
        conn.commit()
        conn.close()
        print("Nothing to fetch.")
        return

    queue: asyncio.Queue = asyncio.Queue()
    for n in to_fetch:
        queue.put_nowait(n)
    for _ in range(concurrency):
        queue.put_nowait(None)

    timeout_obj = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    db_lock = asyncio.Lock()
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        workers = [asyncio.create_task(worker(i, queue, session, api_key, timeout, conn, db_lock))
                   for i in range(concurrency)]
        await queue.join()
        for w in workers:
            await w

    conn.commit()
    conn.close()
    print("Done.")

# ------------------------
# CLI
# ------------------------

def main():
    p = argparse.ArgumentParser(description="Fetch Companies House company profiles into SQLite.")
    p.add_argument("--api-key", required=True, help="Companies House API key")
    p.add_argument("--db", default="company_metadata.sqlite", help="SQLite DB path (created if missing)")
    p.add_argument("--csv", default=None, help="(Optional) CSV with company numbers")
    p.add_argument("--column", default=None, help="Column in CSV; default tries common names")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent requests")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="Per-request socket timeout (seconds)")
    p.add_argument("--force", action="store_true", help="Force refetch of all target company numbers")
    p.add_argument("--since-days", type=int, default=None, help="Refetch rows older than N days")
    args = p.parse_args()

    asyncio.run(run_pipeline(
        api_key=args.api_key,
        db_path=args.db,
        csv_path=args.csv,
        column=args.column,
        concurrency=args.concurrency,
        timeout=args.timeout,
        force=args.force,
        since_days=args.since_days,
    ))

if __name__ == "__main__":
    main()
