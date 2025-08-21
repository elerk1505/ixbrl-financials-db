#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly iXBRL -> Azure SQL (direct upsert into dbo.financials)

- Pulls one or more *monthly* CH bulk zips (YYYY-MM) and parses to rows.
- Normalises key columns so they match the dbo.financials schema you created.
- Upserts straight into dbo.financials (via a temporary staging table inside SQL),
  reusing scripts.azure_sql.upsert_financials_dataframe.

Examples:
  python scripts/ingest_monthly_financials.py --months 2024-06,2024-07
  python scripts/ingest_monthly_financials.py --since-months 6

Requires:
  pip install httpx pandas stream-read-xbrl sqlalchemy pyodbc
"""

from __future__ import annotations
import argparse
import os
from datetime import date
from typing import List, Optional

import httpx
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip

# Re-use your existing SQL helpers
from scripts.azure_sql import upsert_financials_dataframe


def monthly_zip_url(ym: str) -> str:
    # Companies House monthly: Accounts_Bulk_Data-YYYY-MM.zip
    return f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-{ym}.zip"


def fetch_month_as_df(ym: str, timeout: float = 300.0) -> pd.DataFrame:
    """
    Stream a monthly ZIP (YYYY-MM) and parse iXBRL rows to a DataFrame.
    """
    url = monthly_zip_url(ym)
    print(f"Fetching: {url}")
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            # coerce None -> "", keep simple strings, let SQL types be handled later
            data = [[("" if v is None else str(v)) for v in row] for row in rows]
    return pd.DataFrame(data, columns=columns)


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Ensure 'companies_house_registered_number' exists and is stripped of spaces
    - Ensure 'period_end' exists and is a proper date
    - Keep all other columns as-is (your SQL upsert handles the rest)
    """
    # Canonical company number column
    if "companies_house_registered_number" not in df.columns:
        # many feeds use company_number; map it
        if "company_number" in df.columns:
            df = df.rename(columns={"company_number": "companies_house_registered_number"})
        else:
            # best effort: if neither, create blank column to avoid SQL errors
            df["companies_house_registered_number"] = ""

    df["companies_house_registered_number"] = (
        df["companies_house_registered_number"].astype(str).str.replace(" ", "", regex=False)
    )

    # Canonical period_end column (map common variants)
    if "period_end" not in df.columns:
        for cand in ("balance_sheet_date", "date_end", "yearEnd"):
            if cand in df.columns:
                df = df.rename(columns={cand: "period_end"})
                break

    if "period_end" in df.columns:
        # robust parse; keep only the date component
        df["period_end"] = pd.to_datetime(df["period_end"], errors="coerce").dt.date

    # Make column names SQL-friendly
    df = df.rename(columns={c: str(c).strip().replace(" ", "_") for c in df.columns})
    return df


def parse_months_arg(months_arg: Optional[str], since_months: int) -> List[str]:
    """
    Build list of YYYY-MM strings, either from --months CSV or last N months.
    """
    if months_arg:
        out = []
        for raw in months_arg.split(","):
            m = raw.strip()
            if len(m) == 7 and m[4] == "-" and m[:4].isdigit() and m[5:7].isdigit():
                out.append(m)
            else:
                print(f"WARNING: Skipping invalid month format: {raw!r} (expected YYYY-MM)")
        # de-dup while preserving order
        seen = set()
        uniq = [x for x in out if not (x in seen or seen.add(x))]
        return uniq

    # last N calendar months including current month
    today = date.today()
    res: List[str] = []
    y, m = today.year, today.month
    for _ in range(max(1, since_months)):
        res.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y -= 1
            m = 12
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest Companies House monthly iXBRL into dbo.financials")
    ap.add_argument("--months", help="Comma-separated YYYY-MM list (e.g. 2024-01,2024-02).")
    ap.add_argument("--since-months", type=int, default=int(os.getenv("SINCE_MONTHS", "1")),
                    help="If --months not provided, take current month back N-1 months (default 1).")
    ap.add_argument("--timeout", type=float, default=300.0, help="HTTP timeout per file (seconds).")
    args = ap.parse_args()

    months = parse_months_arg(args.months, args.since_months)
    if not months:
        print("Nothing to do (no months resolved).")
        return 0

    any_success = False

    for ym in months:
        try:
            df = fetch_month_as_df(ym, timeout=args.timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                print(f"No monthly file for {ym} (404). Skipping.")
                continue
            print(f"HTTP error for {ym}: {e}")
            return 1
        except Exception as e:
            print(f"Unexpected error fetching {ym}: {e}")
            return 1

        if df.empty:
            print(f"{ym}: ZIP parsed but had 0 rows. Skipping.")
            continue

        df = normalise_columns(df)

        try:
            # Directly upsert into dbo.financials (this creates/uses a temp stage table internally)
            upsert_financials_dataframe(df, target_table="dbo.financials")
        except Exception as e:
            print(f"SQL upsert error for {ym}: {e}")
            return 1

        print(f"✅ {ym}: upserted {len(df):,} rows into dbo.financials.")
        any_success = True

    if not any_success:
        print("No data ingested. Exiting successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
