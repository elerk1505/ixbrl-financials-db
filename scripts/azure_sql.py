# scripts/azure_sql.py
import os, urllib.parse, uuid
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

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

def ensure_company_profiles_table():
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[company_profiles]') AND type in (N'U'))
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
    with engine().begin() as con:
        return [r[0] for r in con.execute(text("""
            SELECT DISTINCT companies_house_registered_number
            FROM dbo.financials
            WHERE companies_house_registered_number IS NOT NULL
        """))]

def upsert_financials_dataframe(df: pd.DataFrame,
                                target_table: str = "dbo.financials",
                                key_cols=None):
    if df is None or df.empty:
        return
    if key_cols is None:
        key_cols = ["companies_house_registered_number", "period_end"]

    df = df.copy()
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]

    stage = f"_stg_fin_{uuid.uuid4().hex[:8]}"
    eng = engine()
    with eng.begin() as con:
        df.to_sql(stage, con=con, schema="dbo", if_exists="replace",
                  index=False, method="multi", chunksize=1000)
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
