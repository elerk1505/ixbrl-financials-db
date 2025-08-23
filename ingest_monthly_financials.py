#!/usr/bin/env python3
"""
Ingest Companies House monthly iXBRL into Azure SQL.

Usage examples:
  python ingest_monthly_financials.py --start-month 2025-01 --end-month 2025-06 \
      --odbc-connect "DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=45;ConnectRetryCount=3;ConnectRetryInterval=10" \
      --schema dbo --target-table financials

  # Or "last N months"
  python ingest_monthly_financials.py --since-months 6 --odbc-connect "..." --schema dbo --target-table financials
"""
from __future__ import annotations

import argparse
import calendar
import contextlib
import datetime as dt
import io
import os
import re
import sys
import time
import zipfile
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------------
# Helpers: months & URLs
# ----------------------------

def month_range(start_ym: str, end_ym: str) -> List[str]:
    ys, ms = map(int, start_ym.split("-"))
    ye, me = map(int, end_ym.split("-"))
    d = dt.date(ys, ms, 1)
    out = []
    while (d.year, d.month) <= (ye, me):
        out.append(f"{d.year:04d}-{d.month:02d}")
        d = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return out

def last_n_months(n: int) -> List[str]:
    today = dt.date.today().replace(day=1)
    months = []
    for i in range(n):
        d = (today - dt.timedelta(days=1)).replace(day=1) if i == 0 else (dt.date.fromordinal((months[-1] and dt.date.fromisoformat(months[-1]+"-01")).toordinal()) - dt.timedelta(days=1)).replace(day=1)
        if i == 0:
            d = today
        months.append(f"{d.year:04d}-{d.month:02d}")
        today = (today - dt.timedelta(days=1)).replace(day=1)
    return sorted(set(months))

def month_to_urls(ym: str) -> List[str]:
    y, m = map(int, ym.split("-"))
    mon = calendar.month_name[m]
    base = f"Accounts_Monthly_Data-{mon}{y}.zip"
    return [
        f"https://download.companieshouse.gov.uk/{base}",
        f"https://download.companieshouse.gov.uk/archive/{base}",
    ]

# ----------------------------
# Robust HTTP client
# ----------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "ixbrl-ingest/1.0"})
    return s

def download_zip_with_checks(session: requests.Session, url: str, max_attempts: int = 5, timeout: int = 60) -> Optional[bytes]:
    for attempt in range(1, max_attempts + 1):
        try:
            r = session.get(url, stream=True, timeout=timeout)
            # If it truly is a 404, no point retrying this URL.
            if r.status_code == 404:
                return None
            r.raise_for_status()
            expected = r.headers.get("Content-Length")
            expected_size = int(expected) if (expected and expected.isdigit()) else None

            buf = io.BytesIO()
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                buf.write(chunk)

            data = buf.getvalue()
            if expected_size is not None and len(data) != expected_size:
                raise IOError(f"incomplete body: got {len(data)} expected {expected_size}")

            # sanity: ensure it’s a valid zip file
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                z.testzip()  # raises if corrupt
            return data
        except requests.HTTPError as e:
            print(f"    fetch attempt {attempt}/{max_attempts} failed: HTTP {e.response.status_code}", flush=True)
            if e.response is not None and e.response.status_code == 404:
                return None
        except Exception as e:
            print(f"    fetch attempt {attempt}/{max_attempts} failed: {e}", flush=True)
            time.sleep(min(10, attempt * 2))
    return None

# ----------------------------
# iXBRL parsing
# ----------------------------

IX_NAMES = [
    # Common tags across UK-GAAP / IFRS
    # Add more as needed; unknown tags will be ignored gracefully.
    "uk-gaap:TurnoverGrossOperatingRevenue",
    "uk-gaap:GrossProfitLoss",
    "uk-gaap:OperatingProfitLoss",
    "uk-gaap:ProfitLossForPeriod",
    "uk-gaap:AverageNumberEmployeesDuringPeriod",
    "uk-gaap:NetAssetsLiabilitiesIncludingPensionAssetLiability",
    "uk-gaap:TotalAssetsLessCurrentLiabilities",
    "uk-gaap:CashBankOnHand",
    "uk-gaap:CurrentAssets",
    "uk-gaap:CreditorsDueWithinOneYear",
    "uk-gaap:CreditorsDueAfterOneYear",
    "ifrs-full:Revenue",
    "ifrs-full:ProfitLoss",
    "ifrs-full:CashAndCashEquivalents",
    "ifrs-full:AverageNumberOfEmployeesDuringThePeriod",
    "ifrs-full:TotalAssets",
    "ifrs-full:Liabilities",
    "ifrs-full:Equity",
]

# simple regex on <ix:nonFraction ... name="xxx">123</ix:nonFraction>
NONFRACTION_RE = re.compile(rb'<ix:nonFraction[^>]*\sname="([^"]+)"[^>]*>(.*?)</ix:nonFraction>', re.I | re.S)
STRING_RE = re.compile(rb'<ix:nonNumeric[^>]*\sname="([^"]+)"[^>]*>(.*?)</ix:nonNumeric>', re.I | re.S)

def parse_ixbrl_html(html_bytes: bytes) -> Dict[str, Optional[str]]:
    """Return a dict of field -> value for selected IX_NAMES plus a couple of headings if present."""
    # Extract values
    out: Dict[str, Optional[str]] = {}
    for m in NONFRACTION_RE.finditer(html_bytes):
        name = m.group(1).decode(errors="ignore")
        if name in IX_NAMES:
            # strip tags within value
            raw = m.group(2)
            text = re.sub(rb"<[^>]+>", b"", raw).strip().decode(errors="ignore")
            out[name] = text
    # Some nonNumeric strings (entity name etc.) – heuristic, optional
    for m in STRING_RE.finditer(html_bytes):
        name = m.group(1).decode(errors="ignore")
        if name.lower().endswith(("entitycurrentlegalname", "companieshouseregisterednumber")):
            raw = m.group(2)
            text = re.sub(rb"<[^>]+>", b"", raw).strip().decode(errors="ignore")
            out[name] = text
    return out

def rows_from_zip(data: bytes, ym: str, zip_url: str) -> List[Dict[str, Optional[str]]]:
    rows: List[Dict[str, Optional[str]]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        candidates = [n for n in z.namelist() if n.lower().endswith((".html", ".xhtml", ".htm"))]
        for name in candidates:
            with z.open(name) as f:
                html = f.read()
            fields = parse_ixbrl_html(html)
            if not fields:
                continue
            row = {
                "run_code": ym.replace("-", ""),
                "company_id": fields.get("CompaniesHouseRegisteredNumber") or fields.get("companieshouseregisterednumber"),
                "date": ym + "-01",
                "file_type": "ixbrl",
                "taxonomy": None,
                "balance_sheet_date": None,
                "companies_house_registered_number": fields.get("CompaniesHouseRegisteredNumber") or fields.get("companieshouseregisterednumber"),
                "entity_current_legal_name": fields.get("EntityCurrentLegalName") or fields.get("entitycurrentlegalname"),
                "company_dormant": None,
                "average_number_employees_during_period": fields.get("uk-gaap:AverageNumberEmployeesDuringPeriod") or fields.get("ifrs-full:AverageNumberOfEmployeesDuringThePeriod"),
                "period_start": None,
                "period_end": None,
                "tangible_fixed_assets": None,
                "debtors": None,
                "cash_bank_in_hand": fields.get("uk-gaap:CashBankOnHand") or fields.get("ifrs-full:CashAndCashEquivalents"),
                "current_assets": fields.get("uk-gaap:CurrentAssets"),
                "creditors_due_within_one_year": fields.get("uk-gaap:CreditorsDueWithinOneYear"),
                "creditors_due_after_one_year": fields.get("uk-gaap:CreditorsDueAfterOneYear"),
                "net_current_assets_liabilities": None,
                "total_assets_less_current_liabilities": fields.get("uk-gaap:TotalAssetsLessCurrentLiabilities"),
                "net_assets_liabilities_including_pension_asset_liability": fields.get("uk-gaap:NetAssetsLiabilitiesIncludingPensionAssetLiability"),
                "called_up_share_capital": None,
                "profit_loss_account_reserve": None,
                "shareholder_funds": None,
                "turnover_gross_operating_revenue": fields.get("uk-gaap:TurnoverGrossOperatingRevenue") or fields.get("ifrs-full:Revenue"),
                "other_operating_income": None,
                "cost_sales": None,
                "gross_profit_loss": fields.get("uk-gaap:GrossProfitLoss"),
                "administrative_expenses": None,
                "raw_materials_consumables": None,
                "staff_costs": None,
                "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets": None,
                "other_operating_charges_format2": None,
                "operating_profit_loss": fields.get("uk-gaap:OperatingProfitLoss"),
                "profit_loss_on_ordinary_activities_before_tax": None,
                "tax_on_profit_or_loss_on_ordinary_activities": None,
                "profit_loss_for_period": fields.get("uk-gaap:ProfitLossForPeriod") or fields.get("ifrs-full:ProfitLoss"),
                "zip_url": zip_url,
            }
            rows.append(row)
    return rows

# ----------------------------
# SQL (create/merge) with robust error handling
# ----------------------------

STAGING_TABLE = "financials_stage"

CREATE_STAGING_SQL = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :table
)
BEGIN
    EXEC('CREATE TABLE [' + :schema + '].[' + :table + '] (
        [run_code] NVARCHAR(32) NULL,
        [company_id] NVARCHAR(64) NULL,
        [date] DATE NULL,
        [file_type] NVARCHAR(32) NULL,
        [taxonomy] NVARCHAR(128) NULL,
        [balance_sheet_date] DATE NULL,
        [companies_house_registered_number] NVARCHAR(64) NULL,
        [entity_current_legal_name] NVARCHAR(512) NULL,
        [company_dormant] NVARCHAR(32) NULL,
        [average_number_employees_during_period] NVARCHAR(64) NULL,
        [period_start] DATE NULL,
        [period_end] DATE NULL,
        [tangible_fixed_assets] NVARCHAR(64) NULL,
        [debtors] NVARCHAR(64) NULL,
        [cash_bank_in_hand] NVARCHAR(64) NULL,
        [current_assets] NVARCHAR(64) NULL,
        [creditors_due_within_one_year] NVARCHAR(64) NULL,
        [creditors_due_after_one_year] NVARCHAR(64) NULL,
        [net_current_assets_liabilities] NVARCHAR(64) NULL,
        [total_assets_less_current_liabilities] NVARCHAR(64) NULL,
        [net_assets_liabilities_including_pension_asset_liability] NVARCHAR(64) NULL,
        [called_up_share_capital] NVARCHAR(64) NULL,
        [profit_loss_account_reserve] NVARCHAR(64) NULL,
        [shareholder_funds] NVARCHAR(64) NULL,
        [turnover_gross_operating_revenue] NVARCHAR(64) NULL,
        [other_operating_income] NVARCHAR(64) NULL,
        [cost_sales] NVARCHAR(64) NULL,
        [gross_profit_loss] NVARCHAR(64) NULL,
        [administrative_expenses] NVARCHAR(64) NULL,
        [raw_materials_consumables] NVARCHAR(64) NULL,
        [staff_costs] NVARCHAR(64) NULL,
        [depreciation_other_amounts_written_off_tangible_intangible_fixed_assets] NVARCHAR(64) NULL,
        [other_operating_charges_format2] NVARCHAR(64) NULL,
        [operating_profit_loss] NVARCHAR(64) NULL,
        [profit_loss_on_ordinary_activities_before_tax] NVARCHAR(64) NULL,
        [tax_on_profit_or_loss_on_ordinary_activities] NVARCHAR(64) NULL,
        [profit_loss_for_period] NVARCHAR(64) NULL,
        [zip_url] NVARCHAR(1024) NULL
    )');
END
"""

MERGE_SQL_TEMPLATE = """
-- simple upsert by (run_code, company_id, date)
MERGE [{schema}].[{target}] AS T
USING (SELECT * FROM [{schema}].[{stage}] WHERE run_code = :run_code) AS S
ON (T.run_code = S.run_code AND ISNULL(T.company_id,'') = ISNULL(S.company_id,'') AND T.[date] = S.[date])
WHEN MATCHED THEN UPDATE SET
    {update_cols}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({cols}) VALUES ({vals});
"""

def create_engine_pyodbc(odbc_connect: str) -> Engine:
    # TrustServerCertificate etc should be carried in odbc_connect provided by caller.
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={requests.utils.quote(odbc_connect)}",
        fast_executemany=True,
        pool_pre_ping=True,
        pool_recycle=120,
    )

def ensure_staging(engine: Engine, schema: str, table: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(CREATE_STAGING_SQL),
            {"schema": schema, "table": table},
        )

def clear_stage_for_run(engine: Engine, schema: str, table: str, run_code: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM [{schema}].[{table}] WHERE run_code = :rc"), {"rc": run_code})

def bulk_stage_rows(engine: Engine, schema: str, table: str, rows: List[Dict[str, Optional[str]]]) -> None:
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ", ".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{schema}].[{table}] ({', '.join(f'[{c}]' for c in cols)}) VALUES ({placeholders})"
    values = [[row.get(c) for c in cols] for row in rows]

    # raw_connection to get cursor.executemany with robust rollback
    attempts = 3
    for attempt in range(1, attempts + 1):
        conn = engine.raw_connection()
        try:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.executemany(insert_sql, values)
            conn.commit()
            return
        except pyodbc.OperationalError as e:
            print(f"    stage insert attempt {attempt}/{attempts} failed: {e}", flush=True)
            with contextlib.suppress(Exception):
                conn.rollback()
            if attempt == attempts:
                raise
            time.sleep(5)
        finally:
            with contextlib.suppress(Exception):
                conn.close()

def merge_stage_into_target(engine: Engine, schema: str, stage: str, target: str, run_code: str) -> None:
    # Build update and insert lists dynamically from stage table columns (same as row keys)
    stage_cols = [
        "run_code","company_id","date","file_type","taxonomy","balance_sheet_date",
        "companies_house_registered_number","entity_current_legal_name","company_dormant",
        "average_number_employees_during_period","period_start","period_end","tangible_fixed_assets",
        "debtors","cash_bank_in_hand","current_assets","creditors_due_within_one_year",
        "creditors_due_after_one_year","net_current_assets_liabilities","total_assets_less_current_liabilities",
        "net_assets_liabilities_including_pension_asset_liability","called_up_share_capital",
        "profit_loss_account_reserve","shareholder_funds","turnover_gross_operating_revenue",
        "other_operating_income","cost_sales","gross_profit_loss","administrative_expenses",
        "raw_materials_consumables","staff_costs",
        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
        "other_operating_charges_format2","operating_profit_loss",
        "profit_loss_on_ordinary_activities_before_tax",
        "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period","zip_url"
    ]
    cols_clause = ", ".join(f"[{c}]" for c in stage_cols)
    vals_clause = ", ".join(f"S.[{c}]" for c in stage_cols)
    update_cols = ", ".join(f"T.[{c}] = S.[{c}]" for c in stage_cols if c not in ("run_code","company_id","date"))

    sql = MERGE_SQL_TEMPLATE.format(schema=schema, target=target, stage=stage, update_cols=update_cols, cols=cols_clause, vals=vals_clause)
    with engine.begin() as conn:
        # Ensure target exists (simple create if missing)
        conn.execute(text(f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
               WHERE s.name = :schema AND t.name = :target)
BEGIN
  SELECT TOP 0 * INTO [{schema}].[{target}] FROM [{schema}].[{stage}];
END
"""), {"schema": schema, "target": target})
        conn.execute(text(sql), {"run_code": run_code})

# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--since-months", type=int)
    g.add_argument("--start-month", type=str)
    ap.add_argument("--end-month", type=str)
    ap.add_argument("--odbc-connect", type=str, required=True)
    ap.add_argument("--schema", type=str, default="dbo")
    ap.add_argument("--target-table", type=str, required=True)
    args = ap.parse_args()

    if args.since_months is not None:
        months = month_range(
            (dt.date.today().replace(day=1) - dt.timedelta(days=30 * (args.since_months - 1))).strftime("%Y-%m"),
            dt.date.today().strftime("%Y-%m"),
        )
    else:
        if not args.end_month:
            ap.error("--end-month is required when using --start-month")
        months = month_range(args.start_month, args.end_month)

    print(f"*** --schema {args.schema} --target-table {args.target_table}", flush=True)
    print(f"Target months: {months}", flush=True)

    # SQL engine
    engine = create_engine_pyodbc(args.odbc_connect)
    ensure_staging(engine, args.schema, STAGING_TABLE)

    session = make_session()
    total_rows = 0

    for ym in months:
        urls = month_to_urls(ym)
        print(f"Fetching month: {ym} -> {urls[0]}", flush=True)

        data = download_zip_with_checks(session, urls[0])
        if data is None:
            print(f"  {ym}: trying next URL -> {urls[1]}", flush=True)
            data = download_zip_with_checks(session, urls[1])

        if data is None:
            print(f"{ym}: no available monthly ZIP (skipping).", flush=True)
            continue

        try:
            rows = rows_from_zip(data, ym, urls[0])
        except zipfile.BadZipFile:
            # re-download if the body looked fine but zip is corrupt (rare)
            print(f"{ym}: corrupt zip body, retrying alternate URL…", flush=True)
            data = download_zip_with_checks(session, urls[1])
            if not data:
                print(f"{ym}: still corrupt or missing, skipping.", flush=True)
                continue
            rows = rows_from_zip(data, ym, urls[1])

        if not rows:
            print(f"{ym}: parsed 0 iXBRL rows (no ixbrl files or no matching tags), skipping.", flush=True)
            continue

        run_code = ym.replace("-", "")
        print(f"{ym}: staging {len(rows)} rows…", flush=True)
        # cleanup and stage
        try:
            clear_stage_for_run(engine, args.schema, STAGING_TABLE, run_code)
            bulk_stage_rows(engine, args.schema, STAGING_TABLE, rows)
            print(f"{ym}: merging to [{args.schema}].[{args.target_table}]…", flush=True)
            merge_stage_into_target(engine, args.schema, STAGING_TABLE, args.target_table, run_code)
            total_rows += len(rows)
        except Exception as e:
            # Ensure any broken transaction is rolled back; engine.begin() blocks already do this,
            # but raw_connection inserts can leave an invalid txn if error was at SQLEndTran.
            print(f"{ym}: staging/merge error: {e}", flush=True)

    print(f"Ingest complete. Rows ingested: {total_rows}.", flush=True)
    return 0 if total_rows > 0 else 0  # non-zero only if you want to fail when nothing found

if __name__ == "__main__":
    sys.exit(main())
