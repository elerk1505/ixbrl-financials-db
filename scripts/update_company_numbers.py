#!/usr/bin/env python3
from scripts.azure_sql import distinct_company_numbers_from_financials
if __name__ == "__main__":
    ids = distinct_company_numbers_from_financials()
    print(f"{len(ids)} ids (from Azure).")
