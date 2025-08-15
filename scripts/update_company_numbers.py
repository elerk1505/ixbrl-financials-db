# scripts/update_company_numbers_sqlite.py
import argparse, os, sqlite3
from pathlib import Path
import pandas as pd

CANDIDATE_ID_COLS = ["company_number","companies_house_registered_number","company_id","companieshouseid"]

def find_id_col(con, table):
    cols = [r[1].lower() for r in con.execute(f"PRAGMA table_info('{table}')")]
    for c in CANDIDATE_ID_COLS:
        if c in cols: return cols[cols.index(c)]
    return None

def extract_ids_from_sqlite(path: Path) -> pd.Series:
    with sqlite3.connect(path) as con:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for t in tables:
            col = find_id_col(con, t)
            if not col: continue
            s = pd.read_sql(f'SELECT DISTINCT "{col}" AS company_number FROM "{t}"', con)["company_number"]
            return s.astype(str).str.replace(" ", "", regex=False)
    return pd.Series(dtype=str)

def ensure_schema(con: sqlite3.Connection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS company_numbers (
      company_number TEXT PRIMARY KEY
    );
    """)
    con.commit()

def upsert_ids(db_path: Path, ids: pd.Series):
    if ids.empty: return 0
    with sqlite3.connect(db_path) as con:
        ensure_schema(con)
        cur = con.cursor()
        cur.execute("CREATE TEMP TABLE _ids(company_number TEXT PRIMARY KEY);")
        # fast bulk insert into temp then merge
        cur.executemany("INSERT OR IGNORE INTO _ids(company_number) VALUES (?)", [(x,) for x in ids.tolist()])
        cur.execute("""
            INSERT OR IGNORE INTO company_numbers(company_number)
            SELECT company_number FROM _ids;
        """)
        cur.execute("DROP TABLE _ids;")
        con.commit()
        return cur.rowcount

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", default=["yearly_sqlites"], help="Folders containing *.sqlite")
    ap.add_argument("--db", required=True, help="Path to data/company_metadata.sqlite")
    args = ap.parse_args()

    inputs = []
    for root in args.inputs:
        p = Path(root)
        if p.is_file() and p.suffix == ".sqlite":
            inputs.append(p)
        elif p.is_dir():
            inputs.extend(sorted(p.glob("*.sqlite")))
    if not inputs:
        print("WARNING: no .sqlite files found in inputs")
    all_ids = []
    for p in inputs:
        try:
            s = extract_ids_from_sqlite(p)
            if not s.empty:
                all_ids.append(s)
                print(f"OK  {p.name}: +{len(s):,} ids")
            else:
                print(f"SKIP {p.name}: no id column found")
        except Exception as e:
            print(f"SKIP {p.name}: {e}")

    if all_ids:
        s = pd.concat(all_ids, ignore_index=True)
        s = s[s.str.len() > 0].drop_duplicates()
    else:
        s = pd.Series(dtype=str)

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    added = upsert_ids(db_path, s)
    print(f"Upserted {len(s):,} IDs (unique). DB: {db_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
