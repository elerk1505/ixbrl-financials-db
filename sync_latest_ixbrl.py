import os
import pandas as pd
import httpx
from stream_read_xbrl import stream_read_xbrl_zip
from datetime import datetime

# Today's date
today = datetime.now().strftime("%Y-%m-%d")
zip_url = f"https://download.companieshouse.gov.uk/Accounts_Bulk_Data-" + today + ".zip"
print("📦 Fetching:", zip_url)

try:
    with httpx.stream("GET", zip_url, timeout=60.0) as r:
        r.raise_for_status()
        with stream_read_xbrl_zip(r.iter_bytes()) as (columns, rows):
            df = pd.DataFrame(rows, columns=columns)
            df = df.applymap(lambda x: str(x) if x is not None else "")
except Exception as e:
    print("❌ Failed to parse:", e)
    exit(1)

# Ensure output directory exists
os.makedirs("yearly_csvs_cleaned", exist_ok=True)

# Save by year
df["balance_sheet_date"] = pd.to_datetime(df["balance_sheet_date"], errors="coerce")
df = df.dropna(subset=["balance_sheet_date"])
df["year"] = df["balance_sheet_date"].dt.year

for year, df_year in df.groupby("year"):
    year_file = f"yearly_csvs_cleaned/{year}.csv"
    if os.path.exists(year_file):
        df_existing = pd.read_csv(year_file)
        df_combined = pd.concat([df_existing, df_year])
        df_combined = df_combined.sort_values("balance_sheet_date")
        df_combined = df_combined.drop_duplicates(subset="companies_house_registered_number", keep="last")
        df_combined.to_csv(year_file, index=False)
    else:
        df_year.to_csv(year_file, index=False)

print("✅ Done updating yearly files.")
