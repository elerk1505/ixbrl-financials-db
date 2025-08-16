#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
from pathlib import Path

SRC = Path("yearly_sqlites/2023.sqlite")   # <-- change year if needed
TABLE = "financials"

def ensure_schema_like(src_con: sqlite3.Connection, dst_path: Path, table: str):
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(dst_path) as dst:
        cur = src_con.execute(f'PRAGMA table_info("{table}")')
        cols = [r[1] for r in cur.fetchall()]
        if not cols:
            raise SystemExit(f"No table '{table}' in {SRC}")
        cols_sql = ", ".join([f'"{c}"' for c in cols])
        dst.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols_sql})')
        # indexes
        dst.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_company_period ON "{table}"(company_number, period_end)')
        dst.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_company ON "{table}"(company_number)')
        dst.execute(f'CREATE INDEX IF NOT EXISTS ix_{table}_period ON "{table}"(period_end)')
        dst.commit()

def main():
    if not SRC.exists():
        raise SystemExit(f"Source not found: {SRC}")

    with sqlite3.connect(SRC) as src:
        # sanity: table exists and has period_end
        cols = [r[1].lower() for r in src.execute(f'PRAGMA table_info("{TABLE}")')]
        if "period_end" not in cols:
            raise SystemExit(f"Column 'period_end' not found in {TABLE} of {SRC}")

        y = int(SRC.stem) if SRC.stem.isdigit() else None
        if not y:
            # try to read min/max year from data
            y = src.execute(f"SELECT strftime('%Y', period_end) FROM {TABLE} WHERE period_end IS NOT NULL LIMIT 1").fetchone()
            if y and y[0] and y[0].isdigit():
                y = int(y[0])
            else:
                raise SystemExit("Could not determine year; rename SRC or set year manually.")

        dst_h1 = SRC.parent / f"{y}_1.sqlite"
        dst_h2 = SRC.parent / f"{y}_2.sqlite"
        ensure_schema_like(src, dst_h1, TABLE)
        ensure_schema_like(src, dst_h2, TABLE)

        # Copy H1 (Jan–Jun)
        print("→ Writing H1 (Jan–Jun)")
        with sqlite3.connect(dst_h1) as d1:
            d1.execute(f'ATTACH DATABASE "{SRC}" AS srcdb')
            d1.execute(f'INSERT OR IGNORE INTO "{TABLE}" SELECT * FROM srcdb."{TABLE}" WHERE CAST(STRFTIME("%m", period_end) AS INTEGER) BETWEEN 1 AND 6')
            d1.execute('DETACH DATABASE srcdb')
            d1.commit()

        # Copy H2 (Jul–Dec)
        print("→ Writing H2 (Jul–Dec)")
        with sqlite3.connect(dst_h2) as d2:
            d2.execute(f'ATTACH DATABASE "{SRC}" AS srcdb')
            d2.execute(f'INSERT OR IGNORE INTO "{TABLE}" SELECT * FROM srcdb."{TABLE}" WHERE CAST(STRFTIME("%m", period_end) AS INTEGER) BETWEEN 7 AND 12')
            d2.execute('DETACH DATABASE srcdb')
            d2.commit()

        print("✅ Split complete:", dst_h1.name, "and", dst_h2.name)

    # Optional: remove the original after verifying the new files
    # os.remove(SRC)
    # print("Deleted original:", SRC)

if __name__ == "__main__":
    main()
