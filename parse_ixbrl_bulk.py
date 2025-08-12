# parse_ixbrl_bulk.py — updated with multiprocessing fix and resume support

import os
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip
import httpx
from datetime import datetime

# List of HTTPS ZIP URLs for the last 36 months
ZIP_URLS = [
    f"https://download.companieshouse.gov.uk/{'archive/' if '2025' not in month else ''}Accounts_Monthly_Data-{month}.zip"
    for month in [
        "August2025", "July2025", "June2025", "May2025", "April2025", "March2025",
        "February2025", "January2025", "December2024", "November2024", "October2024",
        "September2024", "August2024", "July2024", "June2024", "May2024", "April2024",
        "March2024", "February2024", "January2024", "December2023", "November2023",
        "October2023", "September2023", "August2023", "July2023", "June2023",
        "May2023", "April2023", "March2023", "February2023", "January2023",
        "December2022", "November2022", "October2022"
    ]
]

OUTPUT_CSV = "parsed_ixbrl_3y.csv"
PROGRESS_LOG = "parsed_months.txt"

# Ensure progress log exists
if not os.path.exists(PROGRESS_LOG):
    with open(PROGRESS_LOG, "w") as f:
        f.write("")

with open(PROGRESS_LOG, "r") as f:
    completed = set(line.strip() for line in f.readlines())

def process_zip(zip_url):
    month_key = zip_url.split("-")[-1].replace(".zip", "")
    if month_key in completed:
        print(f"⏩ Skipping already processed: {month_key}")
        return

    print(f"\n📂 Parsing: {zip_url}")
    try:
        with httpx.stream("GET", zip_url, timeout=60.0) as r:
            r.raise_for_status()
            with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
                df = pd.DataFrame(rows, columns=columns)
                df = df.applymap(lambda x: str(x) if x is not None else "")
                df.to_csv(OUTPUT_CSV, mode="a", header=not os.path.exists(OUTPUT_CSV), index=False)
                with open(PROGRESS_LOG, "a") as f:
                    f.write(month_key + "\n")
                print(f"✅ Added {len(df)} rows")
    except Exception as e:
        print(f"⚠️ Skipping {zip_url}: {e}")

if __name__ == "__main__":
    for zip_url in ZIP_URLS:
        process_zip(zip_url)

    print("\n✅ Finished saving", OUTPUT_CSV)
