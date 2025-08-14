#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_company_numbers.py

Keeps company_numbers.csv up to date by scanning one or more folders
containing financial CSVs you keep adding (streaming/bulk extracts, etc.).

It looks for either column:
  - "companies_house_registered_number" (preferred for your financials)
  - "company_number"

It de-duplicates and preserves order (new IDs appended to the bottom).

Usage:
  python update_company_numbers.py \
      --inputs ./financials_2022 ./financials_2023 ./daily_drops \
      --output company_numbers.csv

Notes:
- Only reads *.csv files by default (use --pattern to change).
- For large folders, you can run this repeatedly; it's idempotent.
"""
import argparse
import csv
from pathlib import Path

def scan_ids_from_csv(path: Path, preferred_cols=("companies_house_registered_number","company_number")):
    ids = []
    try:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            # resolve column
            header_lc = {h.lower(): h for h in reader.fieldnames or []}
            col = None
            for cand in preferred_cols:
                if cand in header_lc:
                    col = header_lc[cand]
                    break
            if not col:
                return ids
            for row in reader:
                v = (row.get(col) or "").strip().replace(" ", "")
                if v:
                    ids.append(v)
    except Exception:
        # skip bad files quietly
        pass
    return ids

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True, help="One or more folders to scan for CSVs")
    ap.add_argument("--output", required=True, help="Output path for company_numbers.csv")
    ap.add_argument("--pattern", default="*.csv", help="Glob pattern for files (default: *.csv)")
    args = ap.parse_args()

    folders = [Path(p) for p in args.inputs]
    out = Path(args.output)

    # Load existing output (if present) to preserve prior order
    existing = []
    if out.exists():
        with out.open(newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            header_lc = {h.lower(): h for h in r.fieldnames or []}
            col = header_lc.get("company_number") or header_lc.get("companies_house_registered_number")
            if col:
                for row in r:
                    v = (row.get(col) or "").strip().replace(" ", "")
                    if v:
                        existing.append(v)

    seen = set(existing)
    merged = list(existing)

    for folder in folders:
        if not folder.exists():
            continue
        for path in folder.rglob(args.pattern):
            ids = scan_ids_from_csv(path)
            for cid in ids:
                if cid not in seen:
                    merged.append(cid)
                    seen.add(cid)

    # Write output (always with header "company_number")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["company_number"])
        for cid in merged:
            w.writerow([cid])

    print(f"Wrote {len(merged)} unique company numbers to {out}")

if __name__ == "__main__":
    main()
