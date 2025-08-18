#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily iXBRL -> Azure SQL

- Fetches today's Companies House daily ZIP:
    https://download.companieshouse.gov.uk/Accounts_Bulk_Data-YYYY-MM-DD.zip
  (On Tuesdays this single file already contains Sat+Sun+Mon.)
- If the ZIP is not published yet (HTTP 404), exits SUCCESSFULLY (code 0)
  so GitHub Actions doesn't email errors.
- Optional: --grace-yesterday will try yesterday if today returns 404.
- Optional: --date YYYY-MM-DD to fetch a specific day manually.

Requires:
  pip install httpx pandas stream-read-xbrl
  scripts.azure_sql.upsert_financials_dataframe(DataFrame) -> int
"""

from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip
from scripts.azure_sql import upsert_financials_dataframe


def zip_url_for(date_str: str) -> str:
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{date_str}.zip"


def fetch_zip_as_df(date_str: str, timeout: float = 120.0) -> pd.DataFrame:
    """
    Returns a DataFrame or raises httpx.HTTPStatusError for non-2xx.
    """
    url = zip_url_for(date_str)
    print(f"Fetching: {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            # rows is an iterator: collect into list of lists, stringifying Nones
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    df = pd.DataFrame(data, columns=columns)
    return df


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light normalisation to stabilise key columns before upsert.
    """
    if "company_number" in df.columns:
        df["company_number"] = df["company_number"].astype(str).str.replace(" ", "", regex=False)
    elif "companies_house_registered_number" in df.columns:
        df["company_number"] = df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)

    # standardise 'period_end'
    for cand in ("period_end", "balance_sheet_date", "date_end", "yearEnd"):
        if cand in df.columns:
            if cand != "period_end":
                df = df.rename(columns={cand: "period_end"})
            break

    # Best-effort parse for period_end
    if "period_end" in df.columns:
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce")

    return df


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch today's CH daily ZIP and upsert to Azure SQL.")
    ap.add_argument("--date", help="Fetch specific date (YYYY-MM-DD). Default: today (UTC).")
    ap.add_argument(
        "--grace-yesterday",
        action="store_true",
        help="If today 404s, try yesterday before exiting."
    )
    ap.add_argument("--timeout", type=float, default=120.0, help="HTTP timeout in seconds.")
    args = ap.parse_args()

    # Determine target date (UTC)
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD")
            return 2
    else:
        target_date = datetime.now(timezone.utc).date()

    date_str = target_date.strftime("%Y-%m-%d")

    # Try today
    try:
        df = fetch_zip_as_df(date_str, timeout=args.timeout)
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status == 404:
            print(f"No daily file yet for {date_str} (HTTP 404).")
            if args.grace_yesterday:
                # one more try with yesterday
                yday = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"Trying yesterday ({yday}) due to --grace-yesterday …")
                try:
                    df = fetch_zip_as_df(yday, timeout=args.timeout)
                    date_str = yday  # note which date we ingested
                except httpx.HTTPStatusError as e2:
                    if e2.response.status_code == 404:
                        print(f"No file for {yday} either (404). Exiting successfully.")
                        return 0  # soft-success
                    print(f"HTTP error for {yday}: {e2}. Exiting with failure.")
                    return 1
                except Exception as e2:
                    print(f"Error while fetching {yday}: {e2}. Exiting with failure.")
                    return 1
            else:
                # Normal, e.g. Sundays/Mondays, or too-early UTC—exit successfully
                return 0
        else:
            print(f"HTTP error while fetching {date_str}: {e}")
            return 1
    except Exception as e:
        print(f"Unexpected error while fetching {date_str}: {e}")
        return 1

    if df.empty:
        print(f"{date_str}: ZIP parsed but contained 0 rows. Exiting successfully.")
        return 0

    # Normalise and upsert
    df = normalise_columns(df)

    try:
        inserted = upsert_financials_dataframe(df)
    except Exception as e:
        print(f"Upsert to Azure SQL failed: {e}")
        return 1

    print(f"✅ {date_str}: upserted ~{inserted} rows into Azure SQL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
