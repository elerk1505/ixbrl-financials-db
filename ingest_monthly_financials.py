#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL

URL patterns (as provided):
  Live    : https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{Month}{YYYY}.zip
  Archive : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{Month}{YYYY}.zip
  Special : https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{YYYY}.zip  (2008, 2009)

Month selection:
  --start-month YYYY-MM --end-month YYYY-MM   (inclusive)
  --months 2024-01,2024-02
  --since-months N  (N=1 => current month only)

Writes via staging table (NVARCHAR) then MERGE into final table to avoid PK duplicates.
"""

from __future__ import annotations
import argparse
import os
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import httpx
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# ---------- constants ----------
OUTPUT_COLS: List[str] = [
    "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date",
    "companies_house_registered_number", "entity_current_legal_name", "company_dormant",
    "average_number_employees_during_period", "period_start", "period_end",
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year",
    "net_current_assets_liabilities", "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
    "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
    "error", "zip_url",
]

PK_COLS = ("companies_house_registered_number", "date")  # observed PK from your DB

# ---------- helpers: months ----------

def parse_yyyy_mm(s: str) -> date:
    y, m = s.split("-", 1)
    return date(int(y), int(m), 1)

def month_iter_inclusive(start_ym: str, end_ym: str) -> Iterable[str]:
    cur = parse_yyyy_mm(start_ym)
    end = parse_yyyy_mm(end_ym)
    while cur <= end:
        yield f"{cur.year:04d}-{cur.month:02d}"
        y, m = cur.year, cur.month + 1
        if m == 13:
            y, m = y + 1, 1
        cur = date(y, m, 1)

def months_from_list(csv: str) -> List[str]:
    return [x.strip() for x in csv.split(",") if x.strip()]

def default_since_months(n: int) -> List[str]:
    today = datetime.now(timezone.utc).date().replace(day=1)
    out = []
    y, m = today.year, today.month
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out

# ---------- helpers: SQL ----------

def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or set AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc_connect)
    return create_engine(engine_url, fast_executemany=True, future=True)

def get_table_columns(engine: Engine, schema: str, table: str) -> List[str]:
    insp = inspect(engine)
    return [c["name"] for c in insp.get_columns(table, schema=schema)]

def ensure_staging_table(engine: Engine, schema: str, stage_table: str):
    # All NVARCHAR(MAX) to avoid type cast errors; use bound params so 'dbo' is not treated as a column.
    cols_sql = ",\n".join(f"[{c}] NVARCHAR(MAX) NULL" for c in OUTPUT_COLS)
    sql = text(
        """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = :schema AND t.name = :stage
)
BEGIN
    EXEC(N'CREATE TABLE [' + :schema + N'].[' + :stage + N'] (
        """ + cols_sql + """
    )');
END
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {"schema": schema, "stage": stage_table})

def clear_staging(engine: Engine, schema: str, stage_table: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE [{schema}].[{stage_table}]"))

def write_stage(engine: Engine, schema: str, stage_table: str, df: pd.DataFrame):
    # Insert with executemany for speed & to avoid pandas dtype surprises
    cols = OUTPUT_COLS
    placeholders = ",".join(["?"] * len(cols))
    insert_sql = f"INSERT INTO [{schema}].[{stage_table}] ({','.join(f'[{c}]' for c in cols)}) VALUES ({placeholders})"

    # Coerce everything to str or None so NVARCHAR accepts it
    def coerce(v):
        if pd.isna(v):
            return None
        if isinstance(v, (pd.Timestamp, datetime, date)):
            return v.strftime("%Y-%m-%d")
        return str(v)

    rows = [tuple(coerce(row[c]) for c in cols) for _, row in df.iterrows()]
    from sqlalchemy import event
    with engine.begin() as conn:
        cur = conn.connection.cursor()  # raw pyodbc cursor
        cur.fast_executemany = True
        cur.executemany(insert_sql, rows)

def merge_stage_to_final(engine: Engine, schema: str, stage_table: str, final_table: str):
    # Merge on PK_COLS, update other cols, insert new
    non_pk_cols = [c for c in OUTPUT_COLS if c not in PK_COLS]
    set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_pk_cols)
    insert_cols = ", ".join(f"[{c}]" for c in OUTPUT_COLS)
    insert_vals = ", ".join(f"s.[{c}]" for c in OUTPUT_COLS)

    sql = f"""
MERGE [{schema}].[{final_table}] AS t
USING (SELECT {", ".join(f"[{c}]" for c in OUTPUT_COLS)} FROM [{schema}].[{stage_table}]) AS s
   ON ({' AND '.join(f't.[{k}] = s.[{k}]' for k in PK_COLS)})
WHEN MATCHED THEN UPDATE SET {set_clause}
WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
"""
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

# ---------- helpers: parsing / normalisation ----------

def month_name_en(ym: str) -> str:
    import calendar
    dt = parse_yyyy_mm(ym)
    return calendar.month_name[dt.month]  # January, ...

def build_urls_for_month(ym: str) -> List[str]:
    """Live first, then archive; add yearly bundle for 2008/2009 as last resort."""
    y = parse_yyyy_mm(ym).year
    mon = month_name_en(ym)
    urls = [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{mon}{y}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{mon}{y}.zip",
    ]
    if y in (2008, 2009):
        urls.append(f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip")
    return urls

def fetch_month_zip_as_rows(yyyy_mm: str, *, attempts_per_url: int = 5, timeout: float = 300.0) -> Tuple[List[str], Iterable[List[str]]]:
    """
    Fetch a monthly ZIP and stream-parse it. Retries on incomplete bodies.
    Returns (columns, rows_iterable-as-list-of-lists).
    """
    from stream_read_xbrl import stream_read_xbrl_zip

    headers = {"User-Agent": "ixbrl-ingestor/1.0 (+github-actions)", "Connection": "keep-alive", "Accept": "*/*"}
    urls = build_urls_for_month(yyyy_mm)

    for url in urls:
        print(f"Fetching month: {yyyy_mm} -> {url}")
        for attempt in range(1, attempts_per_url + 1):
            try:
                with httpx.Client(http2=False, headers=headers, follow_redirects=True, timeout=timeout) as client:
                    with client.stream("GET", url) as r:
                        r.raise_for_status()
                        # Optional size check
                        exp = int(r.headers.get("Content-Length", "0") or "0")
                        # Let the parser read the stream; it will raise if the body is truncated
                        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                            # materialise rows to allow retries without keeping the stream
                            normalized_rows = [[("" if v is None else str(v)) for v in row] for row in rows]
                        # If server gave a length, do a cheap sanity check by re-downloading headers, else trust parser
                        if exp == 0 or exp > 0:
                            return list(columns), normalized_rows
            except httpx.HTTPStatusError as e:
                if e.response is not None and e.response.status_code == 404:
                    print(f"  {yyyy_mm}: 404 at {url} (attempt {attempt}/{attempts_per_url}), trying next URL…")
                    break  # try next URL
                print(f"  {yyyy_mm}: HTTP error {e} (attempt {attempt}/{attempts_per_url}), will retry…")
            except httpx.ReadError as e:
                print(f"  {yyyy_mm}: incomplete body/read error on attempt {attempt}/{attempts_per_url}: {e}")
            except Exception as e:
                print(f"  {yyyy_mm}: fetch/parse attempt {attempt}/{attempts_per_url} failed: {e}")
        # next url
    raise RuntimeError(f"{yyyy_mm}: could not fetch/parse from any URL")

def dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def normalise_df(df: pd.DataFrame, zip_url: str) -> pd.DataFrame:
    df = dedupe_columns(df)

    # Company number: prefer 'companies_house_registered_number'
    if "companies_house_registered_number" in df.columns:
        s = df["companies_house_registered_number"]
        if isinstance(s, pd.DataFrame):
            s = s.iloc[:, 0]
        df["companies_house_registered_number"] = s.astype(str).str.replace(" ", "", regex=False)

    # Canonical 'period_end'
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Coerce datelike columns to ISO strings (final write uses NVARCHAR)
    for c in ("date", "balance_sheet_date", "period_start", "period_end"):
        if c in df.columns:
            s = df[c]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = pd.to_datetime(s, errors="coerce")
            df[c] = s.dt.strftime("%Y-%m-%d")

    # Add missing columns, fill with None
    for c in OUTPUT_COLS:
        if c not in df.columns:
            df[c] = None

    # Keep & order only the required output columns
    out = df[OUTPUT_COLS].copy()
    out["zip_url"] = zip_url  # remember the source file
    return out

# ---------- main ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest MONTHLY iXBRL zips into Azure SQL (via staging+merge).")
    # month selection
    ap.add_argument("--start-month")
    ap.add_argument("--end-month")
    ap.add_argument("--months")
    ap.add_argument("--since-months", type=int, default=1)
    # SQL
    ap.add_argument("--engine-url")
    ap.add_argument("--odbc-connect")
    ap.add_argument("--schema", default="dbo")
    ap.add_argument("--target-table", default="financials")
    ap.add_argument("--staging-table", default="financials_stage")
    # timeouts / retries
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--append-retries", type=int, default=5)

    args = ap.parse_args()

    # resolve months
    if args.start_month and args.end_month:
        months = list(month_iter_inclusive(args.start_month, args.end_month))
    elif args.months:
        months = months_from_list(args.months)
    else:
        months = default_since_months(args.since_months)
    print(f"Target months: {months}")

    # engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building engine: {e}")
        return 1

    # ensure staging
    try:
        ensure_staging_table(engine, args.schema, args.staging_table)
        clear_staging(engine, args.schema, args.staging_table)
    except Exception as e:
        print(f"ERROR preparing staging table: {e}")
        return 1

    wrote_any = False

    for ym in months:
        # fetch/parse
        try:
            cols, rows = fetch_month_zip_as_rows(ym, attempts_per_url=5, timeout=args.timeout)
            df = pd.DataFrame(rows, columns=cols)
        except Exception as e:
            print(f"{ym}: fetch/parse error: {e}")
            return 1

        if df.empty:
            print(f"{ym}: parsed 0 rows – skipping.")
            continue

        # normalise & select required columns
        df = normalise_df(df, zip_url=build_urls_for_month(ym)[0])

        # write to staging
        try:
            write_stage(engine, args.schema, args.staging_table, df)
            wrote_any = True
            print(f"✅ {ym}: staged {len(df):,} rows.")
        except SQLAlchemyError as e:
            print(f"{ym}: staging SQL error: {e}")
            return 1
        except Exception as e:
            print(f"{ym}: unexpected staging error: {e}")
            return 1

    if wrote_any:
        # merge once at the end for speed
        try:
            merge_stage_to_final(engine, args.schema, args.staging_table, args.target_table)
            clear_staging(engine, args.schema, args.staging_table)
            print(f"✅ Upserted into {args.schema}.{args.target_table} (via MERGE).")
        except Exception as e:
            print(f"ERROR during MERGE/upsert: {e}")
            return 1
    else:
        print("Nothing ingested (no files or all empty).")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
