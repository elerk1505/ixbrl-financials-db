#!/usr/bin/env python3
import httpx
import pandas as pd
from datetime import datetime
from stream_read_xbrl import stream_read_xbrl_zip
from scripts.azure_sql import upsert_financials_dataframe

def today_zip_url() -> str:
    d = datetime.utcnow().strftime("%Y-%m-%d")
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{d}.zip"

def fetch_today_df() -> pd.DataFrame:
    url = today_zip_url()
    print("Fetching:", url)
    with httpx.stream("GET", url, timeout=120.0) as r:
        r.raise_for_status()
        cols, rows = None, []
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, row_iter):
            cols = columns
            for row in row_iter:
                rows.append([("" if v is None else str(v)) for v in row])
    df = pd.DataFrame(rows, columns=cols)
    return df

def main():
    df = fetch_today_df()
    if df.empty:
        print("No rows today.")
        return 0
    n = upsert_financials_dataframe(df)
    print(f"Upserted approx {n} rows into Azure SQL.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
