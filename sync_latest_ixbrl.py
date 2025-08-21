#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily iXBRL -> Azure SQL

Flow per day:
  1) Download + parse the daily ZIP into a DataFrame
  2) Append to staging table (pandas.to_sql, fast_executemany)
  3) MERGE staging -> dbo.financials on (companies_house_registered_number, period_end)
  4) TRUNCATE staging

Requires:
  pip install httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import httpx
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from stream_read_xbrl import stream_read_xbrl_zip


def zip_url_for(date_str: str) -> str:
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{date_str}.zip"


def fetch_zip_as_df(date_str: str, timeout: float = 120.0) -> pd.DataFrame:
    url = zip_url_for(date_str)
    print(f"Fetching: {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    return pd.DataFrame(data, columns=columns)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    # canonical company_number
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = (
            df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
        )

    # canonical period_end
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    return df


def build_engine(*, engine_url: Optional[str], odbc_connect: Optional[str]) -> Engine:
    if not engine_url and not odbc_connect:
        engine_url = os.getenv("AZURE_SQL_URL")
        odbc_connect = os.getenv("AZURE_SQL_ODBC")
    if not engine_url and not odbc_connect:
        raise RuntimeError("Provide --engine-url or --odbc-connect (or env AZURE_SQL_URL/AZURE_SQL_ODBC).")
    if not engine_url:
        import urllib.parse
        engine_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_connect)}"
    return create_engine(engine_url, fast_executemany=True, future=True)


def infer_staging_dtypes(df: pd.DataFrame) -> Dict[str, "sqlalchemy.types.TypeEngine"]:
    from sqlalchemy import types as sqltypes
    d: Dict[str, sqltypes.TypeEngine] = {}
    for col, dtype in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(dtype):
            d[col] = sqltypes.Date()
        elif pd.api.types.is_object_dtype(dtype):
            d[col] = sqltypes.NVARCHAR(length=None)  # NVARCHAR(MAX)
        else:
            d[col] = sqltypes.NVARCHAR(length=None)
    return d


def to_sql_append(
    df: pd.DataFrame,
    engine: Engine,
    table: str,
    schema: Optional[str] = None,
    chunksize: int = 1000,
) -> int:
    if df.empty:
        return 0
    df = df.rename(columns={c: c.strip().replace(" ", "_") for c in df.columns})
    dtypes = infer_staging_dtypes(df)
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        dtype=dtypes,
        chunksize=chunksize,
    )
    return len(df)


# ----- NEW: MERGE staging -> dbo.financials ----------------------------------

FINAL_TABLE = "dbo.financials"
KEY_COLS = ["companies_house_registered_number", "period_end"]

ALL_COLS = [
    "run_code",
    "company_id",
    "date",
    "file_type",
    "taxonomy",
    "balance_sheet_date",
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_dormant",
    "average_number_employees_during_period",
    "period_start",
    "period_end",
    "tangible_fixed_assets",
    "debtors",
    "cash_bank_in_hand",
    "current_assets",
    "creditors_due_within_one_year",
    "creditors_due_after_one_year",
    "net_current_assets_liabilities",
    "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital",
    "profit_loss_account_reserve",
    "shareholder_funds",
    "turnover_gross_operating_revenue",
    "other_operating_income",
    "cost_sales",
    "gross_profit_loss",
    "administrative_expenses",
    "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2",
    "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
    "error",
    "zip_url",
]

def merge_staging_into_financials(engine: Engine, schema: str, staging_table: str) -> int:
    """MERGE the whole staging table into dbo.financials; return rows affected."""
    cols = ", ".join(f"[{c}]" for c in ALL_COLS)
    insert_vals = ", ".join(f"s.[{c}]" for c in ALL_COLS)

    set_clause = ", ".join(
        f"t.[{c}] = s.[{c}]" for c in ALL_COLS if c not in KEY_COLS
    )
    on_clause = " AND ".join(f"t.[{k}] = s.[{k}]" for k in KEY_COLS)

    sql = f"""
    MERGE {FINAL_TABLE} AS t
    USING (SELECT {cols} FROM [{schema}].[{staging_table}]) AS s
      ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({cols}) VALUES ({insert_vals});
    """

    with engine.begin() as con:
        result = con.execute(text(sql))
        # optional: clear staging after successful merge
        con.execute(text(f"TRUNCATE TABLE [{schema}].[{staging_table}]"))
        return result.rowcount or 0

# -----------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch CH daily ZIP(s) and upsert into dbo.financials.")
    ap.add_argument("--date", help="Fetch specific date (YYYY-MM-DD). If set, only that date.")
    ap.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    ap.add_argument("--lookback-days", type=int, default=1,
                    help="How many days back to try (1=today only, 5=today..-4 days).")
    ap.add_argument("--engine-url", help="SQLAlchemy engine URL (e.g., mssql+pyodbc:///?odbc_connect=...)")
    ap.add_argument("--odbc-connect", help="Raw ODBC connection string (we will url-encode for you).")
    ap.add_argument("--staging-table", default="_stg_fin_2d618a4e",
                    help="Staging table to append into (will be auto-created).")
    ap.add_argument("--schema", default="dbo", help="Schema to use (default dbo).")

    args = ap.parse_args()

    # Build date list
    dates: List[str] = []
    if args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD"); return 2
        dates = [target.strftime("%Y-%m-%d")]
    else:
        start = datetime.now(timezone.utc).date()
        for i in range(args.lookback_days):
            dates.append((start - timedelta(days=i)).strftime("%Y-%m-%d"))

    # Engine
    try:
        engine = build_engine(engine_url=args.engine_url, odbc_connect=args.odbc_connect)
    except Exception as e:
        print(f"ERROR building SQL engine: {e}"); return 1

    any_success = False

    for date_str in dates:
        # 1) Fetch + parse
        try:
            df = fetch_zip_as_df(date_str, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No daily file for {date_str} (404). Skipping."); continue
            print(f"HTTP error for {date_str}: {e}"); return 1
        except Exception as e:
            print(f"Unexpected error while fetching/parsing {date_str}: {e}"); return 1

        if df.empty:
            print(f"{date_str}: ZIP parsed but had 0 rows. Skipping."); continue

        # 2) Normalise
        df = normalise_columns(df)

        # 3) Append to staging
        try:
            rows = to_sql_append(
                df=df,
                engine=engine,
                table=args.staging_table,
                schema=args.schema,
                chunksize=1000,
            )
        except SQLAlchemyError as e:
            print(f"Staging append failed for {date_str}: {args.schema}.{args.staging_table}\nError: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected SQL error for {date_str}: {e}"); return 1

        # 4) Merge staging -> final
        try:
            merged = merge_staging_into_financials(engine, args.schema, args.staging_table)
        except Exception as e:
            print(f"MERGE into {FINAL_TABLE} failed for {date_str}: {e}")
            return 1

        print(f"✅ {date_str}: appended {rows} rows to staging "
              f"and MERGEd {merged} rows into {FINAL_TABLE}.")
        any_success = True

    if not any_success:
        print("No files available in the lookback window. Exiting successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
