#!/usr/bin/env python3
import argparse, calendar, httpx, pandas as pd
from datetime import datetime
from dataclasses import dataclass
from stream_read_xbrl import stream_read_xbrl_zip
from scripts.azure_sql import upsert_financials_dataframe

@dataclass(frozen=True)
class YM:
    y:int; m:int
    def disp(self): return f"{calendar.month_name[self.m]}{self.y}"

def monthly_urls(y,m):
    ym = YM(y,m).disp()
    return [
        f"https://download.companieshouse.gov.uk/Accounts_Monthly_Data-{ym}.zip",
        f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{ym}.zip",
    ]

def annual_2008_2009(y):
    return f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-JanuaryToDecember{y}.zip"

def process_zip(urls) -> int:
    last_e=None; total=0
    for u in urls:
        try:
            print("Fetch:", u)
            with httpx.stream("GET", u, timeout=300.0) as r:
                r.raise_for_status()
                buffer=[]; cols=None
                with stream_read_xbrl_zip(r.iter_bytes()) as (columns, row_iter):
                    cols=columns
                    for row in row_iter:
                        buffer.append([("" if v is None else str(v)) for v in row])
                        if len(buffer)>=200_000:
                            total += flush(buffer, cols); buffer.clear()
                if buffer: total += flush(buffer, cols)
            print("OK:", u, "rows:", total)
            return total
        except Exception as e:
            print("Fail:", u, e); last_e=e
    print("All URLs failed:", last_e)
    return total  # 0 if nothing ingested

def flush(buffer, cols) -> int:
    import pandas as pd
    df = pd.DataFrame(buffer, columns=cols)
    # Make sure upsert_financials_dataframe returns len(df)
    return upsert_financials_dataframe(df)

def iter_months(s,e):
    y1,m1=map(int,s.split("-")); y2,m2=map(int,e.split("-"))
    y,m=y1,m1
    while True:
        yield y,m
        if y==y2 and m==m2: break
        m+=1
        if m==13: y+=1; m=1

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--year", type=int)
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--fail-on-empty", action="store_true", help="exit non-zero if no rows ingested")
    args=ap.parse_args()

    grand_total = 0

    if args.year:
        y = args.year
        if y in (2008, 2009):
            grand_total += process_zip([annual_2008_2009(y)])
        else:
            last_month = 12 if y < datetime.utcnow().year else datetime.utcnow().month
            for m in range(1, last_month+1):
                grand_total += process_zip(monthly_urls(y,m))
        print(f"Total rows ingested for {y}: {grand_total:,}")
        if args.fail_on_empty and grand_total == 0:
            return 2
        return 0

    if not (args.start and args.end):
        ap.error("Use --year Y or --start YYYY-MM --end YYYY-MM")

    for y,m in iter_months(args.start, args.end):
        grand_total += process_zip(monthly_urls(y,m))

    print(f"Total rows ingested for {args.start}..{args.end}: {grand_total:,}")
    if args.fail_on_empty and grand_total == 0:
        return 2
    return 0

if __name__=="__main__":
    raise SystemExit(main())
