#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download today's Companies House daily Accounts ZIP, parse with stream-read-xbrl,
and UPSERT rows into Azure SQL (table: dbo.financials).

Design choices (MVP, robust):
- Store a wide table with flexible columns = NVARCHAR(MAX).
- Always ensure primary keys exist: company_number (NVARCHAR(32)), period_end (DATE).
- Load into a staging table then MERGE into dbo.financials on (company_number, period_end).
- Add any new columns found in the feed automatically (NVARCHAR(MAX)).

Requires env:
  AZ_SQL_SERVER, AZ_SQL_DB, AZ_SQL_USER, AZ_SQL_PWD  (SQL auth)
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

import httpx
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import URL
from stream_read_xbrl import stream_read_xbrl_zip

###############################################################################
# Connection
###############################################################################

SERVER = os.environ.get("AZ_SQL_SERVER")
DB     = os.environ.get("AZ_SQL_DB")
USER   = os.environ.get("AZ_SQL_USER")
PWD    = os.environ.get("AZ_SQL_PWD")

if not (SERVER and DB and USER and PWD):
    print("Missing one of AZ_SQL_SERVER / AZ_SQL_DB / AZ_SQL_USER / AZ_SQL_PWD", file=sys.stderr)
    sys.exit(2)

# ODBC Driver 18
conn_url = URL.create(
    "mssql+pyodbc",
    username=USER,
    password=PWD,
    host=SERVER,
    database=DB,
    query={
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
    },
)
engine = sa.create_engine(conn_url, fast_executemany=True)

TABLE = "financials"
SCHEMA = "dbo"

###############################################################################
# Helpers
###############################################################################

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # company_number
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
    else:
        df["company_number"] = ""

    # standardise period_end
    if "period_end" not in df.columns:
        for cand in ("balance_sheet_date", "date_end", "yearEnd"):
            if cand in df.columns:
                df = df.rename(columns={cand: "period_end"})
                break

    # coerce dates
    if "period_end" in df.columns:
        dt = pd.to_datetime(df["period_end"], errors="coerce")
        df["period_end"] = dt.dt.date

    return df

def ensure_table_and_columns(df: pd.DataFrame, conn: sa.Connection) -> None:
    # Base table with keys if not exists
    conn.execute(sa.text(f"""
    IF OBJECT_ID('{SCHEMA}.{TABLE}', 'U') IS NULL
    BEGIN
      CREATE TABLE {SCHEMA}.{TABLE}(
        company_number NVARCHAR(32) NOT NULL,
        period_end DATE NULL,
        CONSTRAINT PK_{TABLE}_company_period PRIMARY KEY (company_number, period_end)
      );
    END
    """))

    # Add any new columns as NVARCHAR(MAX) (skip the key columns)
    existing = set(r[0].lower() for r in conn.execute(sa.text(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t
    """), {"s": SCHEMA, "t": TABLE}).fetchall())

    for col in df.columns:
        c = col.strip()
        if not c:
            continue
        if c.lower() in existing:
            continue
        if c.lower() in ("company_number", "period_end"):
            continue
        # create as NVARCHAR(MAX) to keep schema flexible
        conn.execute(sa.text(f'ALTER TABLE {SCHEMA}.{TABLE} ADD "{c}" NVARCHAR(MAX) NULL;'))
        existing.add(c.lower())

def stage_and_merge(df: pd.DataFrame, conn: sa.Connection) -> int:
    # Create a temp staging table name unique to this run
    stage = f"stg_financials_{int(datetime.utcnow().timestamp())}"

    # Build staging table with same columns (all NVARCHAR(MAX) except period_end)
    cols = [c for c in df.columns]
    # Create staging
    col_defs = []
    for c in cols:
        if c == "period_end":
            col_defs.append(f'"{c}" DATE NULL')
        else:
            col_defs.append(f'"{c}" NVARCHAR(MAX) NULL')
    col_defs_sql = ", ".join(col_defs)
    conn.execute(sa.text(f'CREATE TABLE {SCHEMA}.{stage} ({col_defs_sql});'))

    # Load data into staging via pandas to_sql
    df.to_sql(stage, con=conn, schema=SCHEMA, if_exists="append", index=False)

    # Build MERGE sets for non-key columns present in df
    non_keys = [c for c in cols if c.lower() not in ("company_number", "period_end")]
    set_list = ", ".join([f'TARGET."{c}" = SOURCE."{c}"' for c in non_keys]) or '/* no non-key cols */ TARGET.company_number = TARGET.company_number'

    # MERGE into live table
    conn.execute(sa.text(f"""
    MERGE {SCHEMA}.{TABLE} AS TARGET
    USING (SELECT * FROM {SCHEMA}.{stage}) AS SOURCE
      ON TARGET.company_number = SOURCE.company_number
     AND TARGET.period_end     = SOURCE.period_end
    WHEN MATCHED THEN
      UPDATE SET {set_list}
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({", ".join(f'"{c}"' for c in cols)})
      VALUES ({", ".join(f'SOURCE."{c}"' for c in cols)});
    """))

    # Row count (approx)
    count = conn.execute(sa.text(f"SELECT COUNT(1) FROM {SCHEMA}.{stage}")).scalar_one()
    conn.execute(sa.text(f"DROP TABLE {SCHEMA}.{stage};"))
    return int(count)

###############################################################################
# Main: download & upsert
###############################################################################

def main() -> int:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{today}.zip"
    print("📦 Fetching:", url, flush=True)

    try:
        with httpx.stream("GET", url, timeout=120.0) as r:
            r.raise_for_status()
            with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                df = pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print("❌ Failed to fetch/parse:", e, file=sys.stderr)
        return 1

    # Normalize
    df = df.applymap(lambda x: "" if x is None else str(x))
    df = normalize_df(df)
    # Require company_number
    df = df[df["company_number"].astype(bool)]
    if df.empty:
        print("No rows to upsert.")
        return 0

    # Upsert
    with engine.begin() as conn:
        ensure_table_and_columns(df, conn)
        n = stage_and_merge(df, conn)

    print(f"✅ Upserted {n:,} rows into {SCHEMA}.{TABLE}.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
