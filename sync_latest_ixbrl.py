import os
import pandas as pd
import httpx
from stream_read_xbrl import stream_read_xbrl_zip
from datetime import datetime

# Today's date
today = datetime.now().strftime("%Y-%m-%d")
zip_url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-" + today + ".zip"
print("📦 Fetching:", zip_url)

try:
    with httpx.stream("GET", zip_url, timeout=60.0) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            df = pd.DataFrame(rows, columns=columns)
            df = df.map(lambda x: str(x) if x is not None else "")  # if df is a Series
except Exception as e:
    print("❌ Failed to parse:", e)
    exit(1)

import sqlite3
from pathlib import Path

def normalize_df(df):
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

def upsert_to_sqlite(df, sqlite_path, table="financials"):
    df = normalize_df(df)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(sqlite_path) as con:
        # Stage the chunk
        df.to_sql("_staging", con, if_exists="replace", index=False)
        cur = con.cursor()
        # Ensure target table exists and add missing columns
        tgt_cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})")]
        stg_cols = [r[1] for r in cur.execute("PRAGMA table_info(_staging)")]
        if not tgt_cols:
            # Build column list first to avoid nested f-string
        columns_formatted = ", ".join(f'"{c}"' for c in stg_cols)
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({columns_formatted})')
            tgt_cols = stg_cols
        for c in stg_cols:
            if c not in tgt_cols:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}"')
        # Unique index for upsert
        if {"company_number", "period_end"}.issubset(stg_cols):
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_company_period ON "{table}"(company_number, period_end)')
            cur.execute(f'INSERT OR REPLACE INTO "{table}" ({", ".join(f"""\"{c}\"""" for c in stg_cols)}) SELECT {", ".join(f"""\"{c}\"""" for c in stg_cols)} FROM _staging')
        else:
            cur.execute(f'INSERT INTO "{table}" ({", ".join(f"""\"{c}\"""" for c in stg_cols)}) SELECT {", ".join(f"""\"{c}\"""" for c in stg_cols)} FROM _staging')
        cur.execute("DROP TABLE IF EXISTS _staging")
        # Helpful indexes
        if "company_number" in stg_cols:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_company ON "{table}"(company_number)')
        if "period_end" in stg_cols:
            cur.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_period ON "{table}"(period_end)')
        con.commit()

# Ensure output directory exists
Path("yearly_sqlites").mkdir(exist_ok=True)

# Save by year to SQLite
df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
df = df.dropna(subset=["balance_sheet_date"])
df["year"] = df["balance_sheet_date"].dt.year

for year, df_year in df.groupby("year"):
    sqlite_file = Path("yearly_sqlites") / f"{year}.sqlite"
    upsert_to_sqlite(df_year, sqlite_file)

print("✅ Done updating yearly SQLite files.")
