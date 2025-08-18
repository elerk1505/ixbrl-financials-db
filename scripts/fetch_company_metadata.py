#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bulk-fetch Companies House company metadata -> Azure SQL (dbo.company_profiles)

Env (set by workflow YAML):
  CH_API_KEY           : Companies House API key (required)
  CONCURRENCY          : parallel workers (default 8)
  TIMEOUT              : per-request socket connect/read timeout seconds (default 20)
  MAX_PER_RUN          : optional cap on number of companies to fetch this run
  SINCE_DAYS           : refresh any profile older than N days
  FORCE_REFETCH        : "1" to refetch everything, ignoring staleness

Relies on scripts.azure_sql:
  - engine()
  - ensure_company_profiles_table()
  - distinct_company_numbers_from_financials()
"""

from __future__ import annotations
import os
import json
import asyncio
import random
import signal
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import aiohttp
from aiohttp import ClientError
from sqlalchemy import text

from scripts.azure_sql import (
    engine,
    ensure_company_profiles_table,
    distinct_company_numbers_from_financials,
)

# Defaults from env (with safe fallbacks)
DEFAULT_CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "8") or "8"))
DEFAULT_TIMEOUT     = max(5, int(os.getenv("TIMEOUT", "20") or "20"))

# Tunables
MAX_RETRIES_PER_ID  = 8
BULK_UPSERT_SIZE    = 500          # flush every N results
GLOBAL_BACKOFF_CAP  = 60.0         # seconds


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_company_number(x: str) -> str:
    # Defensive: CH ids sometimes show up with spaces or lowercase; normalize.
    return (x or "").strip().replace(" ", "").upper()


async def fetch_one(
    session: aiohttp.ClientSession,
    api_key: str,
    company_number: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    """
    Fetch one company JSON with careful retry/backoff.
    Returns dict with keys: status, json, etag, error, company_number
    """
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    auth = aiohttp.BasicAuth(api_key, "")

    # Per-request timeout boundaries are set on the session, but we still
    # pass 'timeout=...' for clarity in newer aiohttp versions.
    for attempt in range(MAX_RETRIES_PER_ID):
        try:
            async with session.get(url, auth=auth, timeout=timeout_seconds) as r:
                text_body = await r.text()
                # Parse as JSON if possible; otherwise store raw body
                try:
                    data = json.loads(text_body)
                except json.JSONDecodeError:
                    data = {"raw": text_body}

                # Success
                if r.status == 200:
                    return {
                        "company_number": company_number,
                        "status": 200,
                        "json": data,
                        "etag": r.headers.get("ETag"),
                        "error": None,
                    }

                # Not found / gone — no point retrying
                if r.status in (404, 410):
                    return {
                        "company_number": company_number,
                        "status": r.status,
                        "json": data,
                        "etag": r.headers.get("ETag"),
                        "error": f"Not found {r.status}",
                    }

                # Rate limited — honor Retry-After if numeric
                if r.status == 429:
                    ra = r.headers.get("Retry-After")
                    if ra and ra.isdigit():
                        delay = min(GLOBAL_BACKOFF_CAP, float(ra))
                    else:
                        delay = min(GLOBAL_BACKOFF_CAP, 2.0 ** attempt)
                    await asyncio.sleep(delay + random.uniform(0, 0.25))
                    continue

                # Transient 5xx
                if 500 <= r.status < 600:
                    delay = min(GLOBAL_BACKOFF_CAP, 2.0 ** attempt)
                    await asyncio.sleep(delay + random.uniform(0, 0.25))
                    continue

                # Other HTTP statuses: return as error (don’t spin forever)
                return {
                    "company_number": company_number,
                    "status": r.status,
                    "json": data,
                    "etag": r.headers.get("ETag"),
                    "error": f"HTTP {r.status}",
                }

        except (ClientError, asyncio.TimeoutError) as e:
            # Network hiccup/timeout; exponential backoff with jitter
            delay = min(GLOBAL_BACKOFF_CAP, 1.5 ** attempt)
            await asyncio.sleep(delay + random.uniform(0, 0.25))
            last_err = repr(e)
            continue

    # Retries exhausted
    return {
        "company_number": company_number,
        "status": None,
        "json": None,
        "etag": None,
        "error": "Retries exhausted",
    }


def select_targets(
    since_days: Optional[int],
    force: bool,
    max_per_run: Optional[int],
) -> List[str]:
    """
    Determine which company numbers to fetch based on existing profiles and staleness.
    """
    ensure_company_profiles_table()

    all_ids = [ _sanitize_company_number(x) for x in distinct_company_numbers_from_financials() ]
    all_ids = [x for x in all_ids if x]  # drop empties

    if not all_ids:
        print("⚠️ No company IDs found in dbo.financials; run the financials ingest first.")
        return []

    with engine().begin() as con:
        existing = { _sanitize_company_number(r[0]) for r in con.execute(text(
            "SELECT company_number FROM dbo.company_profiles"
        )) }

        stale: set[str] = set()
        if since_days:
            q = text(
                "SELECT company_number FROM dbo.company_profiles "
                "WHERE TRY_CONVERT(datetime2, fetched_at) <= DATEADD(day, -:d, SYSUTCDATETIME())"
            )
            stale = { _sanitize_company_number(r[0]) for r in con.execute(q, {"d": int(since_days)}) }

    if force:
        targets = list(all_ids)
    else:
        targets = [n for n in all_ids if (n not in existing) or (n in stale)]

    if max_per_run:
        targets = targets[: int(max_per_run)]

    print(
        f"IDs total:{len(all_ids):,} existing:{len(existing):,} "
        f"stale:{len(stale):,} to_fetch:{len(targets):,}"
    )
    return targets


def upsert_many(rows: List[Dict[str, Any]]) -> None:
    """
    Upsert a batch of results into dbo.company_profiles using a single JSON payload
    and a MERGE statement. Ensures table exists.
    """
    if not rows:
        return

    ensure_company_profiles_table()

    # Normalize rows & serialize JSON payloads
    normalized = []
    for r in rows:
        normalized.append(
            {
                "company_number": _sanitize_company_number(r.get("company_number", "")),
                "fetched_at": _now_iso(),
                "http_status": r.get("status"),
                "etag": r.get("etag"),
                "payload_json": json.dumps(r.get("json")),
                "error": r.get("error"),
            }
        )

    payload = json.dumps(normalized, ensure_ascii=False)

    sql = text(
        """
        DECLARE @j NVARCHAR(MAX) = :payload;

        WITH x AS (
          SELECT
            j.value('(company_number)[1]','nvarchar(64)')    AS company_number,
            j.value('(fetched_at)[1]','nvarchar(64)')        AS fetched_at,
            j.value('(http_status)[1]','int')                AS http_status,
            j.value('(etag)[1]','nvarchar(256)')             AS etag,
            j.value('(payload_json)[1]','nvarchar(max)')     AS payload_json,
            j.value('(error)[1]','nvarchar(4000)')           AS error
          FROM OPENJSON(@j) WITH (obj nvarchar(max) AS JSON)
          CROSS APPLY OPENJSON(obj) WITH (
              company_number nvarchar(64)  '$.company_number',
              fetched_at     nvarchar(64)  '$.fetched_at',
              http_status    int           '$.http_status',
              etag           nvarchar(256) '$.etag',
              payload_json   nvarchar(max) '$.payload_json',
              error          nvarchar(4000)'$.error'
          ) j
        )

        MERGE dbo.company_profiles AS t
        USING x AS s
          ON t.company_number = s.company_number
        WHEN MATCHED THEN UPDATE SET
          fetched_at   = s.fetched_at,
          http_status  = s.http_status,
          etag         = s.etag,
          payload_json = s.payload_json,
          error        = s.error
        WHEN NOT MATCHED THEN
          INSERT(company_number, fetched_at, http_status, etag, payload_json, error)
          VALUES(s.company_number, s.fetched_at, s.http_status, s.etag, s.payload_json, s.error);
        """
    )

    with engine().begin() as con:
        con.execute(sql, {"payload": payload})


async def run(
    api_key: str,
    since_days: Optional[int],
    force: bool,
    max_per_run: Optional[int],
    concurrency: int,
    timeout_seconds: int,
) -> None:
    targets = select_targets(since_days, force, max_per_run)
    if not targets:
        print("Nothing to fetch.")
        return

    # Graceful CTRL-C
    stop_event = asyncio.Event()

    def _handle_sigint():
        if not stop_event.is_set():
            print("🔴 Received interrupt — finishing in-flight tasks then flushing…")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _handle_sigint)
        except NotImplementedError:
            # Windows / limited envs
            pass

    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
    for t in targets:
        queue.put_nowait(t)
    for _ in range(concurrency):
        queue.put_nowait(None)  # sentinels

    results: List[Dict[str, Any]] = []

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=timeout_seconds, sock_read=timeout_seconds)
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        async def worker(idx: int):
            while True:
                if stop_event.is_set() and queue.empty():
                    return
                c = await queue.get()
                try:
                    if c is None:
                        return
                    res = await fetch_one(session, api_key, c, timeout_seconds)
                    results.append(res)

                    if len(results) >= BULK_UPSERT_SIZE:
                        upsert_many(results)
                        results.clear()
                finally:
                    queue.task_done()

        tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
        await queue.join()
        for t in tasks:
            t.cancel()
        # Drain any pending results
        if results:
            upsert_many(results)


def main() -> None:
    api_key = (os.getenv("CH_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("CH_API_KEY env var is required")

    # Parse optional knobs (treat 0/"" as None where it makes sense)
    def _opt_int(name: str) -> Optional[int]:
        raw = os.getenv(name, "")
        if raw is None:
            return None
        raw = raw.strip()
        if raw == "" or raw == "0":
            return None
        try:
            v = int(raw)
            return v if v > 0 else None
        except ValueError:
            return None

    since_days  = _opt_int("SINCE_DAYS")
    max_per_run = _opt_int("MAX_PER_RUN")
    force       = (os.getenv("FORCE_REFETCH", "0").strip() == "1")

    print(
        f"Starting metadata sync at {_now_iso()} "
        f"(concurrency={DEFAULT_CONCURRENCY}, timeout={DEFAULT_TIMEOUT}s, "
        f"since_days={since_days}, max_per_run={max_per_run}, force={force})"
    )

    asyncio.run(
        run(
            api_key=api_key,
            since_days=since_days,
            force=force,
            max_per_run=max_per_run,
            concurrency=DEFAULT_CONCURRENCY,
            timeout_seconds=DEFAULT_TIMEOUT,
        )
    )

    print(f"Done at {_now_iso()}.")

if __name__ == "__main__":
    main()
