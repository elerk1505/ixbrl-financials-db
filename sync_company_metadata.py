#!/usr/bin/env python3
import os, shlex, subprocess, sys
from pathlib import Path

def run(cmd: list[str]) -> None:
    print("+", " ".join(shlex.quote(c) for c in cmd), flush=True)
    subprocess.run(cmd, check=True)

def main() -> int:
    repo = Path(__file__).resolve().parent
    scripts_dir = repo / "scripts"

    updater = scripts_dir / "update_company_numbers_sqlite.py"
    fetcher = scripts_dir / "fetch_company_metadata.py"

    if not updater.exists():
        print(f"ERROR: missing {updater}", file=sys.stderr); return 2
    if not fetcher.exists():
        print(f"ERROR: missing {fetcher}", file=sys.stderr); return 2

    input_dirs = os.getenv("INPUT_DIRS", "yearly_sqlites")
    inputs = input_dirs.split()

    metadata_db = Path(os.getenv("METADATA_DB", "data/company_metadata.sqlite"))
    metadata_db.parent.mkdir(parents=True, exist_ok=True)

    # 1) Upsert IDs into data/company_metadata.sqlite:company_numbers
    run([sys.executable, str(updater), "--inputs", *inputs, "--db", str(metadata_db)])

    # 2) Fetch metadata for NEW/stale companies
    api_key = os.getenv("CH_API_KEY")
    if not api_key:
        print("ERROR: CH_API_KEY is not set", file=sys.stderr); return 3

    concurrency = os.getenv("CONCURRENCY", "8")
    timeout = os.getenv("TIMEOUT", "20")
    since_days = os.getenv("REFRESH_SINCE_DAYS")
    force = os.getenv("FORCE_REFETCH", "0") == "1"

    fetch_cmd = [
        sys.executable, str(fetcher),
        "--api-key", api_key,
        "--db", str(metadata_db),
        "--concurrency", concurrency,
        "--timeout", timeout,
    ]
    if force:
        fetch_cmd.append("--force")
    if since_days and since_days.strip():
        fetch_cmd += ["--since-days", since_days.strip()]

    run(fetch_cmd)
    print("✅ Metadata sync complete.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
