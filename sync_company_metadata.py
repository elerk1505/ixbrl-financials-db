import os
import shlex
import subprocess
import sys
from pathlib import Path

def getenv(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val is not None else default

def run(cmd: list[str]) -> None:
    print("+", " ".join(shlex.quote(c) for c in cmd), flush=True)
    subprocess.run(cmd, check=True)

def main() -> int:
    repo = Path(__file__).resolve().parent
    scripts_dir = repo / "scripts"

    # Resolve helper scripts
    update_script = scripts_dir / "update_company_numbers.py"
    fetch_script = scripts_dir / "fetch_company_metadata.py"

    if not update_script.exists():
        print(f"ERROR: missing {update_script}. Make sure scripts/ is committed.", file=sys.stderr)
        return 2
    if not fetch_script.exists():
        print(f"ERROR: missing {fetch_script}. Make sure scripts/ is committed.", file=sys.stderr)
        return 2

    # Inputs
    input_dirs = getenv("INPUT_DIRS", "financials")
    input_list = input_dirs.split()

    # Outputs
    company_numbers_csv = Path(getenv("COMPANY_NUMBERS_CSV", "data/company_numbers.csv"))
    metadata_db = Path(getenv("METADATA_DB", "data/company_metadata.sqlite"))
    company_numbers_csv.parent.mkdir(parents=True, exist_ok=True)
    metadata_db.parent.mkdir(parents=True, exist_ok=True)
    # Update company_numbers.csv (scan folders for company IDs)
    run([sys.executable, str(update_script), "--inputs", *input_list, "--output", str(company_numbers_csv)])

    # Fetch/refresh metadata
    api_key = getenv("CH_API_KEY")
    if not api_key:
        print("ERROR: CH_API_KEY is not set", file=sys.stderr)
        return 3

    # Controls
    concurrency = getenv("CONCURRENCY", "8")
    timeout = getenv("TIMEOUT", "20")
    since_days = getenv("REFRESH_SINCE_DAYS")  # may be None
    force = getenv("FORCE_REFETCH", "0") == "1"

    fetch_cmd = [
        sys.executable, str(fetch_script),
        "--api-key", api_key,
        "--csv", str(company_numbers_csv),
        "--db", str(metadata_db),
        "--concurrency", concurrency,
        "--timeout", timeout,
    ]
    if force:
        fetch_cmd.append("--force")
    if since_days and since_days.strip():
        fetch_cmd += ["--since-days", since_days.strip()]

    run(fetch_cmd)
    print("Sync complete.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

