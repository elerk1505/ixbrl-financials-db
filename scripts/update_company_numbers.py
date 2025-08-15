#!/usr/bin/env python3
import argparse, os, sqlite3
from pathlib import Path
import pandas as pd

ID_CANDIDATES = [
    "company_number",
    "companies_house_registered_number",
    "company_id",
]

def find_id_column(con, table):
    cols = [r[1].lower() for r in con.execute(f"PRAGMA table_info('{table}')")]
    for c in ID_CANDIDATES:
        if c in cols:
            return c
    return None

def pick_table_with_id(con):
    tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for t in tables:
        col = find_id_column(con, t)
        if col:
            return t, col
    return None, None

def collect_numbers(sqlite_dir: Path):
    nums = []
    for db in sorted(sqlite_dir.glob("*.sqlite")):
        try:
            with sqlite3.connect(db) as con:
                table, id_col = pick_table_with_id(con)
                if not table:
                    print(f"SKIP: no ID column in {db.name}")
                    continue
                q = f'SELECT DISTINCT "{id_col}" FROM "{table}" WHERE "{id_col}" IS NOT NULL'
                rows = [str(r[0]).replace(" ", "") for r in con.execute(q)]
                before = len(nums)
                nums.extend(rows)
                print(f"OK   (+{len(nums)-before:>6} ids) {db.name}:{table}.{id_col}")
        except sqlite3.DatabaseError as e:
            print(f"SKIP: {db} not a database ({e})")
    # dedupe preserving order
    seen, out = set(), []
    for n in nums:
        if n and n not in seen:
            out.append(n); seen.add(n)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", default="yearly_sqlites", help="Folder containing per-year .sqlite files")
    ap.add_argument("--output", default="data/company_numbers.csv")
    args = ap.parse_args()

    in_dir = Path(args.inputs)
    out_csv = Path(args.output)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    if not in_dir.exists():
        print(f"WARNING: input dir not found: {in_dir}")
        pd.DataFrame(columns=["company_number"]).to_csv(out_csv, index=False)
        print(f"Wrote 0 unique company numbers to {out_csv}")
        return

    numbers = collect_numbers(in_dir)
    pd.DataFrame({"company_number": numbers}).to_csv(out_csv, index=False)
    print(f"Wrote {len(numbers)} unique company numbers to {out_csv}")

if __name__ == "__main__":
    main()
