#!/usr/bin/env python3
import os, json, asyncio, aiohttp
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from scripts.azure_sql import engine, ensure_company_profiles_table, distinct_company_numbers_from_financials

DEFAULT_CONCURRENCY = int(os.getenv("CONCURRENCY","8"))
DEFAULT_TIMEOUT     = int(os.getenv("TIMEOUT","20"))

async def fetch_one(session, api_key:str, company_number:str, timeout:int) -> Dict[str,Any]:
    url=f"https://api.company-information.service.gov.uk/company/{company_number}"
    for attempt in range(8):
        try:
            async with session.get(url, timeout=timeout, auth=aiohttp.BasicAuth(api_key,"")) as r:
                txt=await r.text()
                try: data=json.loads(txt)
                except json.JSONDecodeError: data={"raw":txt}
                if r.status==200:
                    return dict(status=200, json=data, etag=r.headers.get("ETag"), error=None)
                if r.status in (404,410):
                    return dict(status=r.status, json=data, etag=r.headers.get("ETag"), error=f"Not found {r.status}")
                if r.status==429:
                    ra=r.headers.get("Retry-After")
                    delay=float(ra) if ra and ra.isdigit() else (2**attempt)
                    await asyncio.sleep(delay); continue
                if 500<=r.status<600:
                    await asyncio.sleep(2**attempt); continue
                return dict(status=r.status, json=data, etag=r.headers.get("ETag"), error=f"HTTP {r.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(min(60,1.5**attempt))
    return dict(status=None, json=None, etag=None, error="Retries exhausted")

def select_targets(since_days: Optional[int], force: bool, max_per_run: Optional[int]) -> List[str]:
    ensure_company_profiles_table()
    all_ids = distinct_company_numbers_from_financials()

    # Guard: nothing to do if financials table is empty
    if not all_ids:
        print("⚠️ No company IDs found in dbo.financials; run the financials ingest first.")
        return []

    with engine().begin() as con:
        existing = {r[0] for r in con.execute(text("SELECT company_number FROM dbo.company_profiles"))}
        stale = set()
        if since_days:
            q = text(
                "SELECT company_number FROM dbo.company_profiles "
                "WHERE fetched_at<=DATEADD(day,-:d, SYSUTCDATETIME())"
            )
            stale = {r[0] for r in con.execute(q, {"d": int(since_days)})}

    if force:
        targets = all_ids
    else:
        targets = [n for n in all_ids if (n not in existing) or (n in stale)]

    if max_per_run:
        targets = targets[:int(max_per_run)]

    print(f"IDs total:{len(all_ids):,} existing:{len(existing):,} stale:{len(stale):,} to_fetch:{len(targets):,}")
    return targets

def upsert_many(rows: List[Dict[str,Any]]):
    if not rows: return
    rows = [
        dict(
            company_number=r["company_number"],
            fetched_at=datetime.now(timezone.utc).isoformat(),
            http_status=r["status"], etag=r["etag"],
            payload_json=json.dumps(r["json"]),
            error=r["error"]
        )
        for r in rows
    ]
    with engine().begin() as con:
        ensure_company_profiles_table()
        # Use table-valued insert via JSON shredding
        con.execute(text("""
        DECLARE @j NVARCHAR(MAX) = :payload;
        WITH x AS (
          SELECT
            j.value('(company_number)[1]','nvarchar(64)') AS company_number,
            j.value('(fetched_at)[1]','nvarchar(64)')     AS fetched_at,
            j.value('(http_status)[1]','int')             AS http_status,
            j.value('(etag)[1]','nvarchar(256)')          AS etag,
            j.value('(payload_json)[1]','nvarchar(max)')  AS payload_json,
            j.value('(error)[1]','nvarchar(4000)')        AS error
          FROM OPENJSON(@j) WITH (obj nvarchar(max) AS JSON)
          CROSS APPLY OPENJSON(obj) WITH (
              company_number nvarchar(64) '$.company_number',
              fetched_at nvarchar(64) '$.fetched_at',
              http_status int '$.http_status',
              etag nvarchar(256) '$.etag',
              payload_json nvarchar(max) '$.payload_json',
              error nvarchar(4000) '$.error'
          ) j
        )
        MERGE dbo.company_profiles AS t
        USING x AS s
          ON t.company_number=s.company_number
        WHEN MATCHED THEN UPDATE SET
          fetched_at = s.fetched_at,
          http_status = s.http_status,
          etag = s.etag,
          payload_json = s.payload_json,
          error = s.error
        WHEN NOT MATCHED THEN
          INSERT(company_number,fetched_at,http_status,etag,payload_json,error)
          VALUES(s.company_number,s.fetched_at,s.http_status,s.etag,s.payload_json,s.error);
        """), {"payload": json.dumps(rows)})

async def run(api_key:str, since_days: Optional[int], force: bool, max_per_run: Optional[int],
              concurrency:int, timeout:int):
    targets = select_targets(since_days, force, max_per_run)
    if not targets:
        print("Nothing to fetch."); return

    queue = asyncio.Queue()
    for t in targets: queue.put_nowait(t)
    for _ in range(concurrency): queue.put_nowait(None)

    timeout_obj = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    results = []

    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        async def worker():
            while True:
                c = await queue.get()
                if c is None: queue.task_done(); return
                res = await fetch_one(session, api_key, c, timeout)
                res["company_number"]=c
                results.append(res)
                if len(results)>=500:
                    upsert_many(results); results.clear()
                queue.task_done()

        tasks=[asyncio.create_task(worker()) for _ in range(concurrency)]
        await queue.join()
        for t in tasks: await t

    if results:
        upsert_many(results)

def main():
    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        raise SystemExit("CH_API_KEY env var is required")
    since_days = int(os.getenv("SINCE_DAYS","0") or 0) or None
    max_per_run = int(os.getenv("MAX_PER_RUN","0") or 0) or None
    force = os.getenv("FORCE_REFETCH","0")=="1"

    asyncio.run(run(api_key, since_days, force, max_per_run, DEFAULT_CONCURRENCY, DEFAULT_TIMEOUT))

if __name__=="__main__":
    main()
