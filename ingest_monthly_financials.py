#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import calendar
import contextlib
import datetime as dt
import io
import logging
import os
import re
import sys
import time
import zipfile
from typing import Iterable, List, Optional, Tuple, Dict

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError

# --------------------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------------------
LOG = logging.getLogger("ingest")
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

# --------------------------------------------------------------------------------------
# URL generation for Companies House “Accounts Monthly Data” ZIPs
# Tries both live and archive, with spaces/underscores variants.
# Examples tried:
#   https://download.companieshouse.gov.uk/Accounts%20Monthly%20Data-January2025.zip
#   https://download.companieshouse.gov.uk/archive/Accounts%20Monthly%20Data-January2025.zip
#   https://download.companieshouse.gov.uk/Accounts_Monthly_Data-January2025.zip
#   https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-January2025.zip
# --------------------------------------------------------------------------------------
SPACE = "%20"

def month_label(year: int, month: int) -> str:
    return f"{calendar.month_name[month]}{year}"

def candidate_urls(year: int, month: int) -> List[str]:
    mon = calendar.month_name[month]
    label = f"{mon}{year}"
    bases = [
        "https://download.companieshouse.gov.uk",
        "https://download.companieshouse.gov.uk/archive",
    ]
    names = [
        f"Accounts{SPACE}Monthly{SPACE}Data-{label}.zip",
        f"Accounts_Monthly_Data-{label}.zip",
    ]
    return [f"{b}/{n}" for b in bases for n in names]

# --------------------------------------------------------------------------------------
# HTTP fetching with retries + content-length verification
# --------------------------------------------------------------------------------------
def http_get_with_retries(urls: List[str], expected_min_len: int = 1024,
                          max_attempts: int = 5, timeout: int = 60) -> Optional[bytes]:
    session = requests.Session()
    session.headers.update({"User-Agent": "ixbrl-ingest/1.0"})
    for url in urls:
        for attempt in range(1, max_attempts + 1):
            try:
                with session.get(url, stream=True, timeout=timeout) as r:
                    if r.status_code == 404:
                        LOG.info("  404 at %s, trying next URL…", url)
                        break  # go to next candidate URL
                    r.raise_for_status()
                    content = io.BytesIO()
                    size = 0
                    for chunk in r.iter_content(chunk_size=1024 * 64):
                        if chunk:
                            content.write(chunk)
                            size += len(chunk)
                    # Content-length guard (many of your earlier runs got size 0)
                    cl = int(r.headers.get("Content-Length", "0"))
                    if cl and size != cl:
                        raise IOError(f"incomplete body: got {size} expected {cl}")
                    if size < expected_min_len:
                        raise IOError(f"too small: {size} bytes")
                    LOG.info("  fetched %s (%d bytes)", url, size)
                    return content.getvalue()
            except requests.HTTPError as e:
                LOG.warning("  fetch attempt %s/%s failed: %s", attempt, max_attempts, e)
            except Exception as e:
                LOG.warning("  fetch attempt %s/%s failed: %s", attempt, max_attempts, e)
            if attempt < max_attempts:
                time.sleep(2 * attempt)
        # next candidate URL if all attempts failed
    return None

# --------------------------------------------------------------------------------------
# iXBRL parsing utilities
# We keep this pragmatic: pull out key fields safely from common tags/attributes.
# Any row missing companies house number is skipped to satisfy NOT NULL constraints.
# --------------------------------------------------------------------------------------
IXBRL_TEXT_TAGS = (
    "ix:nonNumeric",
    "ix:nonfraction",
    "ix:nonFraction",
)

def _text(el) -> str:
    return (el.text or "").strip() if el else ""

def find_first_text(soup: BeautifulSoup, qnames: Iterable[str]) -> Optional[str]:
    for q in qnames:
        el = soup.find(q)
        if el and _text(el):
            return _text(el)
    return None

def extract_company_number(soup: BeautifulSoup, filename: str) -> Optional[str]:
    # Common tags seen in UK iXBRL
    # Try explicit tags first
    for name in [
        "uk-ch:CompanyNumber",
        "uk-gaap:CompanyNumber",
        "uk-core:CompanyNumber",
        "entity:CompanyNumber",
    ] + list(IXBRL_TEXT_TAGS):
        el = soup.find(name, attrs={"name": re.compile("(?i)CompanyNumber|CompanyNo")})
        if el and _text(el):
            # extract digits
            m = re.search(r"\b(\d{6,8})\b", _text(el))
            if m:
                return m.group(1)

    # Sometimes it appears as a data element in ix:nonNumeric with name attr
    for ix in soup.find_all(IXBRL_TEXT_TAGS):
        n = (ix.attrs.get("name") or "").lower()
        if "companynumber" in n or "companyno" in n:
            m = re.search(r"\b(\d{6,8})\b", _text(ix))
            if m:
                return m.group(1)

    # Fallback: 8-digit in filename
    m = re.search(r"\b(\d{8})\b", filename)
    if m:
        return m.group(1)
    return None

def extract_period_end(soup: BeautifulSoup) -> Optional[str]:
    # Try common balance sheet / period end labels
    for label in [
        "uk-ch:BalanceSheetDate",
        "uk-gaap:BalanceSheetDate",
        "uk-core:BalanceSheetDate",
        "ifrs-full:BalanceSheetDate",
    ]:
        el = soup.find(label)
        if el and _text(el):
            return _text(el)
    # Try any ix tag that looks like a date value
    for ix in soup.find_all(IXBRL_TEXT_TAGS):
        n = (ix.attrs.get("name") or "").lower()
        if "balancesheetdate" in n or "periodend" in n or "date" in n:
            t = _text(ix)
            if re.search(r"\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}", t):
                return t
    return None

def extract_title(soup: BeautifulSoup) -> Optional[str]:
    # Legal name
    for label in ["uk-ch:EntityCurrentLegalName", "entity:EntityCurrentLegalName", "uk-core:EntityCurrentLegalName"]:
        el = soup.find(label)
        if el and _text(el):
            return _text(el)
    # Fall back to <title>
    if soup.title and soup.title.text:
        return soup.title.text.strip()
    return None

def parse_ixbrl_bytes(xhtml: bytes, source_zip: str, file_in_zip: str) -> Optional[Dict[str, str]]:
    soup = BeautifulSoup(xhtml, "lxml-xml")  # use xml parser to keep namespaces
    num = extract_company_number(soup, file_in_zip)
    if not num:
        return None  # cannot load without company number
    rec = {
        "run_code": dt.datetime.utcnow().strftime("%Y%m%d%H%M%S"),
        "company_id": num,  # for your table compatibility if present
        "companies_house_registered_number": num,
        "entity_current_legal_name": extract_title(soup) or "",
        "balance_sheet_date": extract_period_end(soup) or "",
        "file_type": "iXBRL",
        "taxonomy": "",  # Left blank; add if you want to detect from namespaces
        "zip_url": source_zip,
        # Add more fields here as needed. Anything missing will be NVARCHAR(NULL).
    }
    return rec

def ixbrl_rows_from_zip(zip_bytes: bytes, source_url: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if not name.lower().endswith((".xhtml", ".html", ".htm")):
                continue
            try:
                with zf.open(name) as f:
                    data = f.read()
                rec = parse_ixbrl_bytes(data, source_url, name)
                if rec and rec.get("companies_house_registered_number"):
                    rows.append(rec)
            except zipfile.BadZipFile:
                raise
            except Exception as e:
                LOG.warning("    parse failed for %s: %s", name, e)
    return rows

# --------------------------------------------------------------------------------------
# Month calculation helpers
# --------------------------------------------------------------------------------------
def months_in_range(start: dt.date, end: dt.date) -> List[Tuple[int, int]]:
    out = []
    y, m = start.year, start.month
    last = (end.year, end.month)
    while (y, m) <= last:
        out.append((y, m))
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return out

# --------------------------------------------------------------------------------------
# SQL Server: schema + staging + merge (with retry + rollback)
# --------------------------------------------------------------------------------------
TRANSIENT_SQLSTATES = {"08S01", "40001", "HYT00"}  # link fail, deadlock, timeout

def with_db_retry(fn_desc: str, engine: Engine, sql: str, params: dict = None, max_attempts: int = 4):
    attempt = 1
    while True:
        try:
            with engine.begin() as conn:  # transaction scope
                conn.execute(text(sql), params or {})
            return
        except OperationalError as e:
            msg = str(e.orig)
            # Surface SQLSTATE if present; pyodbc puts it as first token like ('08S01', '...')
            state = None
            m = re.search(r"\('([0-9A-Z]{5})'", msg)
            if m:
                state = m.group(1)
            LOG.warning("[%s] attempt %d failed (%s).", fn_desc, attempt, state or "OperationalError")
            if state not in TRANSIENT_SQLSTATES or attempt >= max_attempts:
                raise
            time.sleep(2 * attempt)
            attempt += 1
        except SQLAlchemyError:
            raise

def ensure_schema_and_tables(engine: Engine, schema: str, stage: str, target: str):
    # Note: all columns NVARCHAR(MAX) except company number which we keep smaller + NOT NULL in staging
    ddl = f"""
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = :schema)
    EXEC('CREATE SCHEMA [{schema}]');

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :stage
)
BEGIN
    EXEC('CREATE TABLE [{schema}].[{stage}] (
        [run_code] NVARCHAR(32) NULL,
        [company_id] NVARCHAR(32) NULL,
        [companies_house_registered_number] NVARCHAR(16) NOT NULL,
        [balance_sheet_date] NVARCHAR(64) NULL,
        [file_type] NVARCHAR(32) NULL,
        [taxonomy] NVARCHAR(128) NULL,
        [entity_current_legal_name] NVARCHAR(MAX) NULL,
        [zip_url] NVARCHAR(MAX) NULL
    )');
END;

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :target
)
BEGIN
    EXEC('CREATE TABLE [{schema}].[{target}] (
        [run_code] NVARCHAR(32) NULL,
        [company_id] NVARCHAR(32) NULL,
        [companies_house_registered_number] NVARCHAR(16) NOT NULL,
        [balance_sheet_date] NVARCHAR(64) NULL,
        [file_type] NVARCHAR(32) NULL,
        [taxonomy] NVARCHAR(128) NULL,
        [entity_current_legal_name] NVARCHAR(MAX) NULL,
        [zip_url] NVARCHAR(MAX) NULL,
        CONSTRAINT PK_{target}_chno_date PRIMARY KEY CLUSTERED
            (companies_house_registered_number ASC, balance_sheet_date ASC)
    )');
END;
"""
    with_db_retry("ensure schema/tables", engine, ddl, {"schema": schema, "stage": stage, "target": target})

def clear_stage(engine: Engine, schema: str, stage: str):
    with_db_retry("truncate stage", engine, f"TRUNCATE TABLE [{schema}].[{stage}]")

def bulk_stage_insert(engine: Engine, schema: str, stage: str, rows: List[Dict[str, str]]):
    if not rows:
        return
    # Insert using executemany
    cols = [
        "run_code", "company_id", "companies_house_registered_number",
        "balance_sheet_date", "file_type", "taxonomy",
        "entity_current_legal_name", "zip_url"
    ]
    placeholders = ", ".join([f":{c}" for c in cols])
    sql = f"INSERT INTO [{schema}].[{stage}] ({', '.join('['+c+']' for c in cols)}) VALUES ({placeholders})"
    attempt = 1
    max_attempts = 4
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text(sql), rows)
            return
        except OperationalError as e:
            msg = str(e.orig)
            m = re.search(r"\('([0-9A-Z]{5})'", msg)
            state = m.group(1) if m else None
            LOG.warning("[stage insert] attempt %d failed (%s).", attempt, state or "OperationalError")
            if state not in TRANSIENT_SQLSTATES or attempt >= max_attempts:
                raise
            time.sleep(2 * attempt)
            attempt += 1

def merge_stage_into_target(engine: Engine, schema: str, stage: str, target: str):
    # Upsert by (companies_house_registered_number, balance_sheet_date)
    sql = f"""
MERGE [{schema}].[{target}] AS tgt
USING (SELECT * FROM [{schema}].[{stage}]) AS src
ON  tgt.companies_house_registered_number = src.companies_house_registered_number
AND ISNULL(tgt.balance_sheet_date, '') = ISNULL(src.balance_sheet_date, '')
WHEN MATCHED THEN
    UPDATE SET
        tgt.run_code = src.run_code,
        tgt.company_id = src.company_id,
        tgt.file_type = src.file_type,
        tgt.taxonomy = src.taxonomy,
        tgt.entity_current_legal_name = src.entity_current_legal_name,
        tgt.zip_url = src.zip_url
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        run_code, company_id, companies_house_registered_number, balance_sheet_date,
        file_type, taxonomy, entity_current_legal_name, zip_url
    )
    VALUES (
        src.run_code, src.company_id, src.companies_house_registered_number, src.balance_sheet_date,
        src.file_type, src.taxonomy, src.entity_current_legal_name, src.zip_url
    );
"""
    with_db_retry("merge", engine, sql)

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Ingest Companies House iXBRL monthly ZIPs into Azure SQL.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--since-months", type=int, help="Ingest the last N months including current month-1.")
    g.add_argument("--start-month", type=str, help="YYYY-MM for start month (inclusive).")
    p.add_argument("--end-month", type=str, help="YYYY-MM for end month (inclusive).")
    p.add_argument("--odbc-connect", type=str, required=True, help="Full ODBC connection string for SQL Server.")
    p.add_argument("--schema", type=str, default="dbo")
    p.add_argument("--target-table", type=str, default="financials")
    p.add_argument("--staging-table", type=str, default="financials_stage")
    return p.parse_args()

def month_range_from_args(args) -> List[Tuple[int, int]]:
    if args.since_months:
        today = dt.date.today().replace(day=1)
        # last N months counting back from previous month
        last = (today - dt.timedelta(days=1)).replace(day=1)
        first = (last - dt.timedelta(days=31*(args.since_months-1))).replace(day=1)
        return months_in_range(first, last)
    # explicit range
    if not args.end_month:
        raise SystemExit("--end-month is required when using --start-month")
    def parse(s: str) -> dt.date:
        y, m = s.split("-")
        return dt.date(int(y), int(m), 1)
    return months_in_range(parse(args.start_month), parse(args.end_month))

def main() -> int:
    args = parse_args()

    # Engine
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(args.odbc_connect)}", pool_pre_ping=True)

    # Schema + tables
    stage = args.staging_table
    target = args.target_table
    try:
        ensure_schema_and_tables(engine, args.schema, stage, target)
    except Exception as e:
        LOG.error("ERROR: schema inspection / ensure stage failed: %s", e)
        return 1

    months = month_range_from_args(args)
    LOG.info("*** --schema %s --target-table %s", args.schema, target)
    LOG.info("Target months: %s", [f"{y}-{m:02d}" for y, m in months])

    total_rows = 0

    for (year, month) in months:
        label = f"{year}-{month:02d}"
        urls = candidate_urls(year, month)
        LOG.info("Fetching month: %s -> %s", label, urls[0])
        zip_bytes = http_get_with_retries(urls)
        if not zip_bytes:
            LOG.warning("%s: no available monthly ZIP (skipping).", label)
            continue

        try:
            rows = ixbrl_rows_from_zip(zip_bytes, next((u for u in urls), ""))
        except zipfile.BadZipFile as e:
            LOG.error("%s: ZIP error: %s", label, e)
            continue
        except Exception as e:
            LOG.error("%s: fetch/parse error: %s", label, e)
            continue

        # Keep only rows with CH number (avoid NOT NULL staging errors)
        rows = [r for r in rows if r.get("companies_house_registered_number")]
        LOG.info("%s: parsed %d iXBRL rows…", label, len(rows))
        if not rows:
            continue

        try:
            clear_stage(engine, args.schema, stage)
            bulk_stage_insert(engine, args.schema, stage, rows)
            merge_stage_into_target(engine, args.schema, stage, target)
            total_rows += len(rows)
        except Exception as e:
            LOG.error("%s: staging/merge error: %s", label, e)
            # make sure the invalid transaction is rolled back before continuing
            with contextlib.suppress(Exception):
                with engine.connect() as c:
                    c.exec_driver_sql("IF @@TRANCOUNT > 0 ROLLBACK TRAN")
            continue

    LOG.info("Ingest complete. Rows ingested (attempted): %d.", total_rows)
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
