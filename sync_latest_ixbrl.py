#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily iXBRL -> Azure SQL

- Tries a rolling window (default 1 = today only). With --lookback-days N,
  it checks today, yesterday, ... up to N days back, and upserts any that exist.
- If a date 404s, it just skips it (soft success); real errors exit 1.

Requires:
  pip install httpx pandas stream-read-xbrl sqlalchemy pyodbc
  scripts.azure_sql.upsert_financials_dataframe(DataFrame) -> int
"""

from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import httpx
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip
from scripts.azure_sql import upsert_financials_dataframe


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
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)

    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch CH daily ZIP(s) and upsert to Azure SQL.")
    ap.add_argument("--date", help="Fetch specific date (YYYY-MM-DD). If set, only that date.")
    ap.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    ap.add_argument("--lookback-days", type=int, default=1,
                    help="How many days back to try (1=today only, 5=today..-4 days).")
    args = ap.parse_args()

    dates: List[str] = []
    if args.date:
        try:
            target = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD")
            return 2
        dates = [target.strftime("%Y-%m-%d")]
    else:
        start = datetime.now(timezone.utc).date()
        for i in range(args.lookback_days):
            d = (start - timedelta(days=i)).strftime("%Y-%m-%d")
            dates.append(d)

    any_success = False

    for date_str in dates:
        try:
            df = fetch_zip_as_df(date_str, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No daily file for {date_str} (404). Skipping.")
                continue
            print(f"HTTP error for {date_str}: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected error while fetching {date_str}: {e}")
            return 1

        if df.empty:
            print(f"{date_str}: ZIP parsed but had 0 rows. Skipping.")
            continue

        df = normalise_columns(df)
        try:
            inserted = upsert_financials_dataframe(df)
        except Exception as e:
            print(f"Upsert to Azure SQL failed for {date_str}: {e}")
            return 1

        print(f"✅ {date_str}: upserted ~{inserted} rows into Azure SQL.")
        any_success = True

    if not any_success:
        print("No files available in the lookback window. Exiting successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
