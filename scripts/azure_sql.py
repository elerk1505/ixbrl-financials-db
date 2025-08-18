# scripts/azure_sql.py
import os, urllib.parse, uuid
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

# ------------------------------
# Connection / engine helpers
# ------------------------------
def build_conn_str() -> str:
    server   = (os.getenv("AZURE_SQL_SERVER") or "").strip()
    database = (os.getenv("AZURE_SQL_DATABASE") or "").strip()
    username = (os.getenv("AZURE_SQL_USERNAME") or "").strip()
    password = (os.getenv("AZURE_SQL_PASSWORD") or "").strip()
    if not (server and database and username and password):
        raise ValueError("Missing AZURE_SQL_* env vars")

    odbc = (
        f"Driver=ODBC Driver 18 for SQL Server;"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc)

def engine(fast: bool = True):
    url = build_conn_str()
    make_url(url)  # validate early
    return create_engine(url, pool_pre_ping=True, fast_executemany=True)

# ------------------------------
# Utility helpers
# ------------------------------
def _table_exists(con, schema: str, table: str) -> bool:
    q = text("""
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = :schema AND t.name = :table
    """)
    return con.execute(q, {"schema": schema, "table": table}).first() is not None

def _latest_staging_financials_table(con) -> str | None:
    """
    Find the most recently created dbo._stg_fin_% table (the daily job
    logs showed tables like dbo._stg_fin_2d618a4e). Return its name or None.
    """
    q = text("""
        SELECT TOP (1) t.name
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = 'dbo' AND t.name LIKE '_stg_fin_%'
        ORDER BY t.create_date DESC
    """)
    row = con.execute(q).first()
    return row[0] if row else None

# ------------------------------
# Schema helpers for metadata
# ------------------------------
def ensure_company_profiles_table():
    ddl = """
    IF NOT EXISTS (
      SELECT * FROM sys.objects
      WHERE object_id = OBJECT_ID(N'[dbo].[company_profiles]') AND type in (N'U')
    )
    CREATE TABLE [dbo].[company_profiles](
      company_number nvarchar(64) NOT NULL PRIMARY KEY,
      fetched_at nvarchar(64)     NOT NULL,
      http_status int             NULL,
      etag nvarchar(256)          NULL,
      payload_json nvarchar(max)  NULL,
      error nvarchar(4000)        NULL
    );
    """
    with engine().begin() as con:
        con.execute(text(ddl))

def distinct_company_numbers_from_financials():
    """
    Primary source: dbo.financials (if it exists).
    Fallback: newest staging table dbo._stg_fin_% (if present).
    Returns [] if neither exists.
    """
    with engine().begin() as con:
        if _table_exists(con, "dbo", "financials"):
            src = "dbo.financials"
        else:
            latest_stg = _latest_staging_financials_table(con)
            if not latest_stg:
                return []
            src = f"dbo.{latest_stg}"

        rows = con.execute(text(f"""
            SELECT DISTINCT companies_house_registered_number
            FROM {src}
            WHERE companies_house_registered_number IS NOT NULL
        """))
        return [r[0] for r in rows]

# ------------------------------
# Upsert for daily financials
# ------------------------------
def upsert_financials_dataframe(
    df: pd.DataFrame,
    target_table: str = "dbo.financials",
    key_cols=None,
) -> int:
    """
    Loads df to a random staging table, MERGEs into target_table on key_cols,
    drops the staging table, returns how many staging rows were processed.
    """
    if df is None or df.empty:
        return 0
    if key_cols is None:
        key_cols = ["companies_house_registered_number", "period_end"]

    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]

    stage = f"_stg_fin_{uuid.uuid4().hex[:8]}"
    eng = engine()

    with eng.begin() as con:
        # 1) load to staging
        df.to_sql(stage, con=con, schema="dbo", if_exists="replace",
                  index=False, method="multi", chunksize=1000)

        # 2) compute how many rows we intend to merge (for logging)
        rowcount = con.execute(text(f"SELECT COUNT(*) FROM dbo.{stage}")).scalar_one()

        # 3) MERGE into final
        cols = list(df.columns)
        on_clause   = " AND ".join([f"t.[{k}] = s.[{k}]" for k in key_cols])
        set_clause  = ", ".join([f"t.[{c}] = s.[{c}]" for c in cols if c not in key_cols])
        insert_cols = ", ".join(f"[{c}]" for c in cols)
        insert_vals = ", ".join(f"s.[{c}]" for c in cols)

        con.execute(text(f"""
            MERGE {target_table} AS t
            USING (SELECT * FROM dbo.{stage}) AS s
              ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
            DROP TABLE dbo.{stage};
        """))

    return int(rowcount)
