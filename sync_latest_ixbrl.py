#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import httpx
from stream_read_xbrl import stream_read_xbrl_zip

# -----------------------------------------------------------------------------
# 1) Download + parse today's bulk ZIP into a DataFrame
# -----------------------------------------------------------------------------
today = datetime.now().strftime("%Y-%m-%d")
zip_url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{today}.zip"
print("📦 Fetching:", zip_url)

try:
    with httpx.stream("GET", zip_url, timeout=60.0) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            df = pd.DataFrame(rows, columns=columns)
            # Use applymap for DataFrame-wide conversion
            df = df.applymap(lambda x: str(x) if x is not None else "")
except Exception as e:
    print("❌ Failed to parse:", e)
    raise SystemExit(1)

# -----------------------------------------------------------------------------
# 2) Helpers to normalize + upsert into SQLite (per-year databases)
# -----------------------------------------------------------------------------
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # company_number
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)

    # standardise period_end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns and "period_end" not in df.columns:
            df = df.rename(columns={cand: "period_end"})
            break
    return df


def upsert_to_sqlite(df: pd.DataFrame, sqlite_path: Path, table: str = "financials") -> None:
    df = normalize_df(df)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(sqlite_path) as con:
        # Stage the chunk from the current DataFrame
        df.to_sql("_staging", con, if_exists="replace", index=False)
        cur = con.cursor()

        # Read target/staging columns
        tgt_cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{table}")')]
        stg_cols = [r[1] for r in cur.execute('PRAGMA table_info("_staging")')]
        stg_cols_set = set(stg_cols)

        # Create target table if missing (with all staging columns)
        if not tgt_cols:
            columns_formatted = ", ".join([f'"{c}"' for c in stg_cols]) or "dummy INTEGER"
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns_formatted})')
            tgt_cols = stg_cols

        # Add any missing columns
        for c in stg_cols:
            if c not in tgt_cols:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}"')

        # Prepare column list strings (avoid nested f-strings)
        col_list_sql = ", ".join([f'"{c}"' for c in stg_cols])

        # Unique index for upsert when we have both keys
        if {"company_number", "period_end"}.issubset(stg_cols_set):
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_company_period ON "{table}"(company_number, period_end)')
            cur.execute(f'INSERT OR REPLACE INTO "{table}" ({col_list_sql}) SELECT {col_list_sql} FROM "_staging"')
        else:
            cur.execute(f'INSERT INTO "{table}" ({col_list_sql}) SELECT {col_list_sql} FROM "_staging"')

        # Drop staging and add helpful indexes
        cur.execute('DROP TABLE IF EXISTS "_staging"')
        if "company_number" in stg_cols_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_company ON "{table}"(company_number)')
        if "period_end" in stg_cols_set:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_period ON "{table}"(period_end)')

        con.commit()

# -----------------------------------------------------------------------------
# 3) Partition by year and write per-year SQLite databases
# -----------------------------------------------------------------------------
Path("yearly_sqlites").mkdir(exist_ok=True)

# Make sure we have a proper date column to derive the year
df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
df = df.dropna(subset=["balance_sheet_date"])
df["year"] = df["balance_sheet_date"].dt.year

for year, df_year in df.groupby("year"):
    sqlite_file = Path("yearly_sqlites") / f"{int(year)}.sqlite"
    print(f"📝 Upserting year {int(year)} → {sqlite_file}")
    upsert_to_sqlite(df_year, sqlite_file)

print("✅ Done updating yearly SQLite files.")
