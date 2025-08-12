# parse_ixbrl_bulk.py — full version from 2008 to Oct 2022

import os
import pandas as pd
from stream_read_xbrl import stream_read_xbrl_zip
import httpx

OUTPUT_CSV = "parsed_ixbrl_pre2023.csv"
PROGRESS_LOG = "parsed_months2.txt"

# All relevant ZIP months (2008 to October 2022)
months = [
    "JanToDec2008",
    "JanToDec2009",
] + [
    f"{month}{year}"
    for year in range(2010, 2023)
    for month in [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    if not (year == 2022 and month not in ["January", "February", "March", "April", "May", "June",
                                           "July", "August", "September", "October"])
]

# Generate full URLs
ZIP_URLS = [
    f"https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-{month}.zip"
    for month in months
]

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

    print(f"\n✅ Finished saving {OUTPUT_CSV}")
