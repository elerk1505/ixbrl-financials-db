# scripts/azure_sql.py
import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from typing import Optional

# Toggle batch performance only for pyodbc
def engine(fast: bool = True):
    url = build_conn_str()
    # Validate early; raises ArgumentError if malformed
    make_url(url)
    # fast_executemany is only effective for mssql+pyodbc
    kwargs = {"pool_pre_ping": True}
    if url.startswith("mssql+pyodbc://"):
        kwargs["fast_executemany"] = fast
    return create_engine(url, **kwargs)

def build_conn_str() -> str:
    """
    Priority:
      1) AZURE_SQL_CONN – must be a valid SQLAlchemy URL (not raw ODBC)
      2) Construct DSN-less mssql+pyodbc URL from SERVER/DB/USER/PASS envs
    """
    conn = (os.getenv("AZURE_SQL_CONN") or "").strip()
    if conn:
        # Accept either:
        #   mssql+pyodbc://user:pass@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no
        # or:
        #   mssql+pyodbc:///?odbc_connect=<urlencoded ODBC string>
        return conn

    server   = os.getenv("AZURE_SQL_SERVER", "").strip()
    database = os.getenv("AZURE_SQL_DATABASE", "").strip()
    username = os.getenv("AZURE_SQL_USERNAME", "").strip()
    password = os.getenv("AZURE_SQL_PASSWORD", "").strip()

    if not (server and database and username and password):
        raise ValueError(
            "Missing one or more required env vars: AZURE_SQL_SERVER, AZURE_SQL_DATABASE, "
            "AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD (or provide AZURE_SQL_CONN)."
        )

    # Build a DSN-less ODBC connect string and URL-encode it
    odbc = (
        f"Driver=ODBC Driver 18 for SQL Server;"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    odbc_enc = urllib.parse.quote_plus(odbc)
    return f"mssql+pyodbc:///?odbc_connect={odbc_enc}"

# ------- optional helpers you already import elsewhere -------
from sqlalchemy import text

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
        # Adjust table/column names to your schema
        return [r[0] for r in con.execute(text("SELECT DISTINCT companies_house_registered_number FROM dbo.financials WHERE companies_house_registered_number IS NOT NULL"))]
