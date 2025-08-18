#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, text

def build_conn_str() -> str:
    # If a full connection string was provided, use it.
    env_conn = os.getenv("AZURE_SQL_CONN")
    if env_conn:
        return env_conn

    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    user = os.getenv("AZURE_SQL_USERNAME")
    pwd = os.getenv("AZURE_SQL_PASSWORD")

    if not all([server, database, user, pwd]):
        raise RuntimeError("Missing one of AZURE_SQL_SERVER/DB/USERNAME/PASSWORD")

    # ODBC Driver 18 recommended on GitHub runners
    return (
        f"mssql+pyodbc://{user}:{pwd}"
        f"@{server}.database.windows.net:1433/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
    )

def engine(fast=True):
    return create_engine(build_conn_str(), fast_executemany=fast, pool_pre_ping=True)

def ensure_financials_table():
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[financials]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[financials](
            [company_number] NVARCHAR(32) NULL,
            [period_end]     DATE NULL
            -- other columns will be auto-added by MERGE when present (we add via ALTERs below)
        );
        CREATE INDEX IX_fin_company ON [dbo].[financials]([company_number]);
        CREATE INDEX IX_fin_period  ON [dbo].[financials]([period_end]);
        CREATE UNIQUE INDEX UX_fin_company_period ON [dbo].[financials]([company_number],[period_end]);
    END
    """
    with engine().begin() as con:
        con.execute(text(ddl))

def ensure_company_profiles_table():
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[company_profiles]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[company_profiles](
            [company_number] NVARCHAR(32) NOT NULL PRIMARY KEY,
            [fetched_at]     DATETIME2 NULL,
            [http_status]    INT NULL,
            [etag]           NVARCHAR(256) NULL,
            [payload_json]   NVARCHAR(MAX) NULL,
            [error]          NVARCHAR(4000) NULL
        );
        CREATE INDEX IX_cp_status     ON [dbo].[company_profiles]([http_status]);
        CREATE INDEX IX_cp_fetched_at ON [dbo].[company_profiles]([fetched_at]);
    END
    """
    with engine().begin() as con:
        con.execute(text(ddl))

def upsert_financials_dataframe(df, table="financials", key_cols=("company_number","period_end")):
    """
    Robust upsert:
    1) create temp table
    2) bulk-load via to_sql
    3) MERGE into target on (company_number, period_end)
    4) add missing columns dynamically
    """
    from sqlalchemy import text
    import pandas as pd

    if df.empty:
        return 0

    # Normalize keys if present
    for k in ("company_number",):
        if k in df.columns:
            df[k] = df[k].astype(str).str.replace(" ", "", regex=False)
    if "balance_sheet_date" in df.columns and "period_end" not in df.columns:
        df = df.rename(columns={"balance_sheet_date": "period_end"})
    if "period_end" in df.columns:
        # make as date string YYYY-MM-DD so SQL can convert
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    with engine().begin() as con:
        # Ensure main table exists with basic structure
        ensure_financials_table()

        # Create a temp staging table with all incoming columns
        cols_sql = ", ".join(f"[{c}] NVARCHAR(MAX) NULL" for c in df.columns)
        con.execute(text(f"IF OBJECT_ID('tempdb..#staging_fin') IS NOT NULL DROP TABLE #staging_fin;"))
        con.execute(text(f"CREATE TABLE #staging_fin ({cols_sql});"))

        # Bulk insert into temp using to_sql -> into a real table, then SELECT into temp
        # Simpler approach: create a persistent staging table name unique per run
        import uuid
        stg_name = f"_staging_fin_{uuid.uuid4().hex[:8]}"
        con.execute(text(f"CREATE TABLE [{stg_name}] ({cols_sql});"))

        df.to_sql(stg_name, con.connection, if_exists="append", index=False)

        # Add missing columns in target
        existing_cols = [r[0].lower() for r in con.execute(text(
            "SELECT c.name FROM sys.columns c JOIN sys.objects o ON c.object_id=o.object_id WHERE o.name=:t"),
            {"t": table}).fetchall()]
        for c in df.columns:
            if c.lower() not in existing_cols:
                con.execute(text(f'ALTER TABLE [{table}] ADD [{c}] NVARCHAR(MAX) NULL;'))

        # Now MERGE
        nonkey_cols = [c for c in df.columns if c.lower() not in {k.lower() for k in key_cols}]
        set_clause = ", ".join([f"t.[{c}] = s.[{c}]" for c in nonkey_cols]) or "/* no updates */"
        insert_cols = ", ".join([f"[{c}]" for c in df.columns])
        insert_vals = ", ".join([f"s.[{c}]" for c in df.columns])

        merge_sql = f"""
        MERGE [{table}] AS t
        USING (SELECT * FROM [{stg_name}]) AS s
          ON {" AND ".join([f"t.[{k}] = s.[{k}]" for k in key_cols if k in df.columns])}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
        DROP TABLE [{stg_name}];
        """
        con.execute(text(merge_sql))
        # No rowcount from MERGE here; return df len as approximation
        return len(df)

def distinct_company_numbers_from_financials():
    q = text("SELECT DISTINCT company_number FROM dbo.financials WHERE company_number IS NOT NULL")
    with engine().begin() as con:
        rows = con.execute(q).fetchall()
    return [r[0] for r in rows]
