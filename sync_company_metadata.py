import pandas as pd
import httpx
import time
import os

API_KEY = os.getenv("CH_API_KEY")  # Set as a GitHub secret
API_URL = "https://api.company-information.service.gov.uk/company/"

# Load all company numbers from your yearly CSVs
def get_unique_company_numbers(data_dir="yearly_csvs_cleaned"):
    all_files = [
        os.path.join(data_dir, f) for f in os.listdir(data_dir)
        if f.endswith(".csv")
    ]
    company_numbers = set()
    for file in all_files:
        df = pd.read_csv(file, usecols=["companies_house_registered_number"])
        company_numbers.update(df["companies_house_registered_number"].dropna().astype(str))
    return sorted(company_numbers)

# Load existing metadata cache (if any)
def load_existing_metadata(path="company_metadata.csv"):
    if os.path.exists(path):
        return pd.read_csv(path, dtype=str)
    return pd.DataFrame(columns=[
        "companies_house_registered_number", "company_name", "date_of_creation", "company_type", "sic_codes"
    ])

# Fetch metadata from Companies House API
def fetch_metadata(company_number):
    url = API_URL + company_number
    r = httpx.get(url, auth=(API_KEY, ""))
    if r.status_code == 200:
        data = r.json()
        return {
            "companies_house_registered_number": company_number,
            "company_name": data.get("company_name", ""),
            "date_of_creation": data.get("date_of_creation", ""),
            "company_type": data.get("type", ""),
            "sic_codes": "; ".join(data.get("sic_codes", []))
        }
    else:
        print(f"⚠️ Failed {company_number}: {r.status_code}")
        return None

# Main sync function
def sync_metadata():
    existing_df = load_existing_metadata()
    known_ids = set(existing_df["companies_house_registered_number"].astype(str))

    all_ids = get_unique_company_numbers()
    new_ids = [cid for cid in all_ids if cid not in known_ids]

    print(f"🔍 Found {len(new_ids)} new companies to fetch...")

    new_data = []
    for i, cid in enumerate(new_ids, 1):
        meta = fetch_metadata(cid)
        if meta:
            new_data.append(meta)
        if i % 20 == 0:
            time.sleep(1.2)  # Respect rate limits

    if new_data:
        df_new = pd.DataFrame(new_data)
        combined = pd.concat([existing_df, df_new], ignore_index=True)
        combined.drop_duplicates("companies_house_registered_number", keep="last").to_csv(
            "company_metadata.csv", index=False
        )
        print("✅ Metadata updated and saved.")
    else:
        print("✅ No new metadata to update.")

if __name__ == "__main__":
    sync_metadata()
