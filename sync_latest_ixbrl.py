#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import httpx
from stream_read_xbrl import stream_read_xbrl_zip

OUTPUT_DIR = Path("yearly_sqlites")
TABLE = "financials"

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
    # H1 = Jan–Jun -> year_1.sqlite ; H2 = Jul–Dec -> year_2.sqlite
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

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{today}.zip"
    print("📦 Fetching:", url)

    try:
        with httpx.stream("GET", url, timeout=60.0) as r:
            r.raise_for_status()
            with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                df = pd.DataFrame(rows, columns=columns)
                df = df.applymap(lambda x: "" if x is None else str(x))
    except Exception as e:
        print("❌ Failed to parse:", e)
        raise SystemExit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)
    df["balance_sheet_date"] = pd.to_datetime(df.get("balance_sheet_date"), errors="coerce")
    df = df.dropna(subset=["balance_sheet_date"])
    if df.empty:
        print("No dated rows to ingest.")
        return

    # Route to H1/H2 files
    df["year"] = df["balance_sheet_date"].dt.year
    df["half"] = (df["balance_sheet_date"].dt.month <= 6).map({True: 1, False: 2})

    total = 0
    for (year, half), chunk in df.groupby(["year", "half"]):
        db = OUTPUT_DIR / f"{int(year)}_{int(half)}.sqlite"
        print(f"📝 Upserting {len(chunk):,} rows → {db}")
        upsert_df(chunk, db)
        total += len(chunk)
    print(f"✅ Done. Upserted {total:,} rows.")

if __name__ == "__main__":
    main()
