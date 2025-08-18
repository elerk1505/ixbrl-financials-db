# scripts/azure_sql.py
from __future__ import annotations
import os
import pandas as pd
import pyodbc
from typing import Iterable

# ODBC driver name used on GitHub runners after we install MS ODBC 18
DRIVER = "{ODBC Driver 18 for SQL Server}"

def conn_str() -> str:
    server = os.environ["AZURE_SQL_SERVER"]
    db     = os.environ["AZURE_SQL_DATABASE"]
    user   = os.environ["AZURE_SQL_USERNAME"]
    pwd    = os.environ["AZURE_SQL_PASSWORD"]
    # Encrypt is required by Azure; TrustServerCertificate=no is safest
    return (
        f"DRIVER={DRIVER};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"UID={user};PWD={pwd};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

def open_conn() -> pyodbc.Connection:
    return pyodbc.connect(conn_str(), autocommit=False)

def ensure_schema_financials():
    sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'financials')
    BEGIN
        CREATE TABLE dbo.financials (
            company_number NVARCHAR(20) NOT NULL,
            period_end     DATE         NULL,
            -- add/expand columns freely; NVARCHAR(MAX) for wide text is ok
            -- we’ll add dynamically for unseen columns (see upsert)
            CONSTRAINT PK_financials PRIMARY KEY (company_number, period_end)
        );
    END
    """
    with open_conn() as con:
        cur = con.cursor()
        cur.execute(sql)
        con.commit()

def ensure_schema_company_profiles():
    sql = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'company_profiles')
    BEGIN
        CREATE TABLE dbo.company_profiles (
            company_number NVARCHAR(20) PRIMARY KEY,
            fetched_at     DATETIME2     NULL,
            http_status    INT           NULL,
            etag           NVARCHAR(200) NULL,
            payload_json   NVARCHAR(MAX) NULL,
            error          NVARCHAR(4000) NULL
        );
        CREATE INDEX IX_company_profiles_status ON dbo.company_profiles(http_status);
        CREATE INDEX IX_company_profiles_fetched ON dbo.company_profiles(fetched_at);
    END
    """
    with open_conn() as con:
        cur = con.cursor()
        cur.execute(sql)
        con.commit()

def _ensure_columns(table: str, cols: Iterable[str]):
    """Add any missing columns as NVARCHAR(MAX) for simplicity."""
    if not cols:
        return
    with open_conn() as con:
        cur = con.cursor()
        # current columns
        cur.execute(f"""
            SELECT c.name FROM sys.columns c
            JOIN sys.tables t ON c.object_id=t.object_id
            WHERE t.name=?;
        """, table)
        existing = {r[0] for r in cur.fetchall()}
        for c in cols:
            if c in existing or c in ("company_number","period_end"):
                continue
            cur.execute(f'ALTER TABLE dbo.{table} ADD "{c}" NVARCHAR(MAX) NULL;')
        con.commit()

def upsert_financials(df: pd.DataFrame):
    """UPSERT df rows into dbo.financials on (company_number, period_end)."""
    if df.empty: 
        return
    # Normalize
    if "company_number" not in df.columns and "companies_house_registered_number" in df.columns:
        df = df.rename(columns={"companies_house_registered_number":"company_number"})
    df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    if "period_end" not in df.columns:
        for cand in ("balance_sheet_date","date_end","yearEnd"):
            if cand in df.columns:
                df = df.rename(columns={cand:"period_end"})
                break
    # Coerce to ISO strings
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    cols = [c for c in df.columns if c not in ("company_number","period_end")]
    _ensure_columns("financials", cols)

    with open_conn() as con:
        cur = con.cursor()
        # Create staging table
        cur.execute("IF OBJECT_ID('tempdb..#staging') IS NOT NULL DROP TABLE #staging;")
        # Basic schema: keep keys strongly typed, other fields as NVARCHAR(MAX)
        extra = ", ".join([f'[{c}] NVARCHAR(MAX) NULL' for c in cols])
        cur.execute(f"""
            CREATE TABLE #staging (
                company_number NVARCHAR(20) NOT NULL,
                period_end DATE NULL
                {"," if extra else ""} {extra}
            );
        """)
        # Fast bulk insert
        cur.fast_executemany = True
        placeholders = ", ".join(["?"] * len(df.columns))
        insert_sql = f"INSERT INTO #staging ({', '.join('['+c+']' for c in df.columns)}) VALUES ({placeholders})"
        cur.executemany(insert_sql, df.where(pd.notnull(df), None).values.tolist())

        # MERGE for upsert
        set_list = ", ".join([f"T.[{c}]=S.[{c}]" for c in cols])
        merge_sql = f"""
        MERGE dbo.financials AS T
        USING #staging AS S
          ON T.company_number=S.company_number AND T.period_end=S.period_end
        WHEN MATCHED THEN UPDATE SET {set_list}
        WHEN NOT MATCHED THEN INSERT ({', '.join('['+c+']' for c in df.columns)})
                              VALUES ({', '.join('S.['+c+']' for c in df.columns)});
        """
        cur.execute(merge_sql)
        con.commit()

def upsert_company_profile(company_number: str, result: dict):
    ensure_schema_company_profiles()
    with open_conn() as con:
        cur = con.cursor()
        cur.execute("""
        MERGE dbo.company_profiles AS T
        USING (SELECT ? AS company_number) AS S
          ON S.company_number = T.company_number
        WHEN MATCHED THEN UPDATE SET 
            fetched_at=SYSUTCDATETIME(),
            http_status=?,
            etag=?,
            payload_json=?,
            error=?
        WHEN NOT MATCHED THEN INSERT(company_number, fetched_at, http_status, etag, payload_json, error)
            VALUES(?, SYSUTCDATETIME(), ?, ?, ?, ?);
        """, (
            company_number,
            result.get("status"), result.get("etag"),
            result.get("json"),
            result.get("error"),
            company_number,
            result.get("status"), result.get("etag"),
            result.get("json"),
            result.get("error"),
        ))
        con.commit()
