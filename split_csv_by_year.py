import pandas as pd
from pathlib import Path
from datetime import datetime

INPUT_FILE = "parsed_ixbrl_3y.csv"
OUTPUT_DIR = Path("yearly_csvs_cleaned")
OUTPUT_DIR.mkdir(exist_ok=True)

chunksize = 100_000
min_year = 2000
max_year = datetime.now().year + 1

for chunk in pd.read_csv(INPUT_FILE, chunksize=chunksize, low_memory=False):
    # Use balance_sheet_date instead of period_end
    chunk["balance_sheet_date"] = pd.to_datetime(chunk["balance_sheet_date"], errors="coerce")
        # Drop duplicates based on company number + balance sheet date
    chunk = chunk.drop_duplicates(subset=["companies_house_registered_number", "balance_sheet_date"])

    # Drop rows with missing or bad dates
    chunk = chunk.dropna(subset=["balance_sheet_date"])
    chunk["year"] = chunk["balance_sheet_date"].dt.year
    chunk = chunk[(chunk["year"] >= min_year) & (chunk["year"] <= max_year)]

    # Group by year and save
    for year, group in chunk.groupby("year"):
        out_file = OUTPUT_DIR / f"{year}.csv"
        group.drop(columns=["year"]).to_csv(out_file, mode="a", header=not out_file.exists(), index=False)

print("✅ Done — Cleaned yearly CSVs saved to:", OUTPUT_DIR)
