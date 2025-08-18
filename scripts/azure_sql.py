# scripts/azure_sql.py
import os
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

def build_conn_str() -> str:
    """
    Returns a SQLAlchemy-compatible URL for SQL Server via pyodbc.
    Supports:
      - AZURE_SQL_CONN: full ODBC conn string (Driver=...;Server=...;Database=...;Uid=...;Pwd=...;...)
      - OR the 4 separate env vars (server, database, username, password)
    """
    odbc = (os.getenv("AZURE_SQL_CONN") or "").strip()
    if odbc:
        # If a raw ODBC string was provided, wrap it in ?odbc_connect=
        # This avoids URL-escaping headaches and works with special chars.
        if "Driver=" in odbc or "DRIVER=" in odbc:
            return "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(odbc)
        # If it's already a SQLAlchemy URL, just return it.
        return odbc

    # Fall back to individual parts
    server   = (os.getenv("AZURE_SQL_SERVER") or "").strip()
    database = (os.getenv("AZURE_SQL_DATABASE") or "").strip()
    username = (os.getenv("AZURE_SQL_USERNAME") or "").strip()
    password = (os.getenv("AZURE_SQL_PASSWORD") or "").strip()

    if not all([server, database, username, password]):
        raise RuntimeError(
            "Missing SQL settings. Provide AZURE_SQL_CONN (ODBC string) "
            "or AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD."
        )

    # Compose a standard SQLAlchemy URL. Escape user/pass because they may contain special chars.
    q_user = urllib.parse.quote_plus(username)
    q_pwd  = urllib.parse.quote_plus(password)

    # Use the officially installed driver name; keep port 1433 explicit.
    driver_qs = urllib.parse.urlencode({
        "driver": "ODBC Driver 18 for SQL Server",
        "Encrypt": "yes",
        "TrustServerCertificate": "no",
    })

    # For Azure SQL, server should be like "yourserver.database.windows.net"
    # Do NOT include "tcp:" or ",1433" here; port is specified separately.
    return f"mssql+pyodbc://{q_user}:{q_pwd}@{server}:1433/{database}?{driver_qs}"

def engine(fast: bool = True):
    url = build_conn_str()

    # Validate URL early (raises on malformed strings)
    try:
        make_url(url)
    except Exception as e:
        raise RuntimeError(f"Invalid SQLAlchemy URL built: {url}") from e

    return create_engine(url, fast_executemany=fast, pool_pre_ping=True)

def ensure_company_profiles_table():
    # your existing implementation…
    pass

def distinct_company_numbers_from_financials():
    # your existing implementation…
    return []
