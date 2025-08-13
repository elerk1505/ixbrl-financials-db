# sync_company_metadata.py
import os, csv, time, json, math, random, signal
from pathlib import Path
from datetime import datetime, timezone
import httpx

# --- add these near the top (after imports) ---
import sqlite3
import glob

ID_COLS = [
    "companies_house_registered_number",
    "company_number",
    "companyNumber",
    "company_id",
    "companies_house_registered_no",
    "ch_number",
]

def _read_ids_from_csv(path):
    import pandas as pd
    try:
        # Try to read only the first matching column
        for col in ID_COLS:
            try:
                s = pd.read_csv(path, usecols=[col], dtype=str)
                return [x.strip() for x in s[col].dropna().astype(str).tolist()]
            except ValueError:
                continue
        # Fallback: first column
        s = pd.read_csv(path, usecols=[0], header=0, dtype=str)
        first_col = s.columns[0]
        vals = s[first_col].dropna().astype(str).tolist()
        # Skip header‑like values
        if first_col.lower() in ID_COLS:
            return [v.strip() for v in vals]
        return [v.strip() for v in vals if v.lower() not in ID_COLS]
    except Exception:
        return []

def _read_ids_from_sqlite(path, max_rows=10_000_000):
    ids = []
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        # Find tables that contain a CH‑like column
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            # PRAGMA to find columns
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = [r[1] for r in cur.fetchall()]
            match = next((c for c in cols if c in ID_COLS), None)
            if not match:
                continue
            # Stream out distinct IDs
            try:
                for row in cur.execute(f"SELECT DISTINCT {match} FROM '{t}' LIMIT {max_rows}"):
                    if row and row[0]:
                        ids.append(str(row[0]).strip())
            except Exception:
                continue
    except Exception:
        pass
    finally:
        try:
            con.close()
        except Exception:
            pass
    return ids

def _discover_ids_from_repo(glob_spec):
    """
    glob_spec: semicolon‑separated globs, e.g. 'data/**/*.csv;*.sqlite'
    """
    patterns = [g.strip() for g in (glob_spec or "").split(";") if g.strip()]
    if not patterns:
        # sensible defaults for your repo layout (CSV‑first, then SQLite)
        patterns = ["**/*.csv", "**/*.sqlite"]
    found = []
    seen = set()
    for pat in patterns:
        for path in glob.glob(pat, recursive=True):
            if path.lower().endswith(".csv"):
                for cid in _read_ids_from_csv(path):
                    if cid and cid not in seen:
                        seen.add(cid); found.append(cid)
            elif path.lower().endswith(".sqlite") or path.lower().endswith(".db"):
                for cid in _read_ids_from_sqlite(path):
                    if cid and cid not in seen:
                        seen.add(cid); found.append(cid)
    return found

def _load_existing_metadata_ids(output_csv_path):
    # Remove already‑synced IDs so we don’t re‑request them
    if not Path(output_csv_path).exists():
        return set()
    try:
        import pandas as pd
        # Read only the ID column; try the common names
        for col in ["companies_house_registered_number"] + ID_COLS:
            try:
                s = pd.read_csv(output_csv_path, usecols=[col], dtype=str)
                return set(s[col].dropna().astype(str).str.strip().tolist())
            except ValueError:
                continue
    except Exception:
        pass
    return set()

# --- replace your existing load_ids() with this ---
def load_ids():
    """
    Priority:
      1) If NEW_COMPANY_IDS_CSV exists -> use it (backwards compatible)
      2) Else auto‑discover from repo using IDS_SOURCES glob(s)
    After collection, subtract anything already present in OUTPUT_CSV or checkpoint.done
    """
    src_csv = os.environ.get("NEW_COMPANY_IDS_CSV", "new_company_ids.csv")
    discovered = []

    if Path(src_csv).exists():
        print(f"📄 Using IDs from CSV: {src_csv}")
        discovered = _read_ids_from_csv(src_csv)
    else:
        glob_spec = os.environ.get("IDS_SOURCES", "")  # e.g., "data/**/*.csv;**/*.sqlite"
        print("🔎 No new_company_ids.csv — auto‑discovering IDs from repo…")
        discovered = _discover_ids_from_repo(glob_spec)
        print(f"🔎 Discovered {len(discovered)} potential company IDs from repo files")

    # De‑dupe & normalise
    norm = []
    seen = set()
    for cid in discovered:
        if not cid:
            continue
        c = cid.strip()
        if c and c not in seen:
            seen.add(c)
            norm.append(c)

    # Remove already present in output CSV
    already = _load_existing_metadata_ids(OUTPUT_CSV)

    # Remove already done from checkpoint if present
    done_map = {}
    if Path(CHECKPOINT).exists():
        try:
            with open(CHECKPOINT, "r", encoding="utf-8") as f:
                done_map = json.load(f).get("done", {}) or {}
        except Exception:
            pass
    already.update(done_map.keys())

    remaining = [c for c in norm if c not in already]
    print(f"🧮 IDs total: {len(norm)} | already synced/skipped: {len(already)} | remaining: {len(remaining)}")
    return remaining

API_KEY = os.environ["COMPANIES_HOUSE_API_KEY"]  # set in workflow secrets
INPUT_NEW_IDS_CSV = os.environ.get("NEW_COMPANY_IDS_CSV", "new_company_ids.csv")
OUTPUT_CSV = os.environ.get("METADATA_OUT_CSV", "company_metadata.csv")
CHECKPOINT = os.environ.get("CHECKPOINT_FILE", "sync_metadata.progress.json")

# Pace: CH ~600 req / 5 min => 2 rps. Stay conservative.
TARGET_RPS = float(os.environ.get("TARGET_RPS", "2.0"))
MAX_REQUESTS_PER_RUN = int(os.environ.get("MAX_REQUESTS_PER_RUN", "9000"))  # ~75 min at 2 rps
BATCH_FLUSH = int(os.environ.get("BATCH_FLUSH", "200"))  # write CSV every N rows
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "20.0"))  # seconds
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

BASE_URL = "https://api.company-information.service.gov.uk/company/{}"

HEADERS = {
    "User-Agent": "ixbrl-financials-db sync-metadata (GitHub Actions)"
}

def load_checkpoint():
    if not Path(CHECKPOINT).exists():
        return {"done": {}, "index": 0, "requests_made": 0}
    with open(CHECKPOINT, "r", encoding="utf-8") as f:
        return json.load(f)

def save_checkpoint(state):
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, CHECKPOINT)

def csv_writer(path, fieldnames):
    file_exists = Path(path).exists()
    f = open(path, "a", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not file_exists:
        w.writeheader()
    return f, w

def jitter_sleep(seconds):
    # add +/- 20% jitter to avoid burst alignment
    adj = seconds * (0.8 + 0.4 * random.random())
    time.sleep(adj)

def pace(last_request_time):
    if last_request_time is None:
        return time.monotonic()
    elapsed = time.monotonic() - last_request_time
    min_interval = 1.0 / TARGET_RPS
    if elapsed < min_interval:
        jitter_sleep(min_interval - elapsed)
    return time.monotonic()

def should_retry(status_code):
    return status_code in (429, 500, 502, 503, 504)

def get_with_retries(client, url):
    attempt = 0
    while True:
        try:
            resp = client.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS, auth=(API_KEY, ""))
            # Fast path for permanent misses
            if resp.status_code in (404, 410):
                return resp
            if resp.is_success:
                return resp
            # Respect Retry-After
            if should_retry(resp.status_code):
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        wait = float(ra)
                    except ValueError:
                        wait = 5.0
                    jitter_sleep(wait)
                    attempt += 1
                else:
                    # backoff
                    wait = min(30.0, (2 ** attempt))
                    jitter_sleep(wait)
                    attempt += 1
                if attempt > MAX_RETRIES:
                    return resp
            else:
                return resp
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.WriteTimeout):
            wait = min(30.0, (2 ** attempt))
            jitter_sleep(wait)
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
        except httpx.HTTPError:
            # transient transport error, backoff
            wait = min(30.0, (2 ** attempt))
            jitter_sleep(wait)
            attempt += 1
            if attempt > MAX_RETRIES:
                raise

def normalize_company_number(cn: str) -> str:
    # CH expects zero-padded in some ranges; keep original, but ensure no spaces
    return cn.strip()

def sync_metadata():
    print("🔍 Loading new company ids…")
    ids = load_ids()
    print(f"🔍 Found {len(ids)} new companies to fetch...")

    state = load_checkpoint()
    idx = state.get("index", 0)
    requests_made = state.get("requests_made", 0)
    done = state.get("done", {})

    # Prepare CSV writer
    fieldnames = [
        "companies_house_registered_number", "company_name", "company_status", "company_type",
        "date_of_creation", "sic_codes", "registered_office_address_postcode",
        "registered_office_address_locality", "registered_office_address_country",
        "registered_office_is_in_dispute", "can_file", "has_insolvency_history",
        "has_charges", "last_full_members_list_date"
    ]
    f, w = csv_writer(OUTPUT_CSV, fieldnames)

    last_request_time = None

    # graceful shutdown on SIGTERM from Actions
    def _shutdown(signum, frame):
        print("⚠️ Received shutdown signal. Saving checkpoint…")
        save_checkpoint({"done": done, "index": idx, "requests_made": requests_made})
        f.close()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=10)
    transport = httpx.HTTPTransport(retries=0)
    with httpx.Client(limits=limits, transport=transport, http2=False) as client:
        processed_in_batch = 0
        while idx < len(ids) and requests_made < MAX_REQUESTS_PER_RUN:
            cid = normalize_company_number(ids[idx])

            if cid in done:
                idx += 1
                continue

            url = BASE_URL.format(cid)
            last_request_time = pace(last_request_time)

            resp = None
            try:
                resp = get_with_retries(client, url)
            except httpx.ReadTimeout:
                print("⏱️ ReadTimeout (final) — will checkpoint & continue later.")
                break

            requests_made += 1

            if resp.status_code == 404:
                print(f"⚠️ Not found {cid}: 404")
                done[cid] = {"status": 404}
            elif resp.status_code == 410:
                print(f"⚠️ Gone {cid}: 410")
                done[cid] = {"status": 410}
            elif not resp.is_success:
                print(f"⚠️ {cid}: HTTP {resp.status_code} — giving up this run")
                done[cid] = {"status": resp.status_code}
            else:
                data = resp.json()
                row = {
                    "companies_house_registered_number": cid,
                    "company_name": data.get("company_name"),
                    "company_status": data.get("company_status"),
                    "company_type": data.get("type"),
                    "date_of_creation": data.get("date_of_creation"),
                    "sic_codes": ",".join(data.get("sic_codes", []) or []),
                    "registered_office_address_postcode": (data.get("registered_office_address") or {}).get("postal_code"),
                    "registered_office_address_locality": (data.get("registered_office_address") or {}).get("locality"),
                    "registered_office_address_country": (data.get("registered_office_address") or {}).get("country"),
                    "registered_office_is_in_dispute": data.get("registered_office_is_in_dispute"),
                    "can_file": data.get("can_file"),
                    "has_insolvency_history": data.get("has_insolvency_history"),
                    "has_charges": data.get("has_charges"),
                    "last_full_members_list_date": data.get("last_full_members_list_date"),
                }
                w.writerow(row)
                processed_in_batch += 1
                done[cid] = {"status": 200}

                if processed_in_batch >= BATCH_FLUSH:
                    f.flush()
                    os.fsync(f.fileno())
                    save_checkpoint({"done": done, "index": idx + 1, "requests_made": requests_made})
                    processed_in_batch = 0

            idx += 1

    # final flush + checkpoint
    f.flush()
    os.fsync(f.fileno())
    f.close()
    save_checkpoint({"done": done, "index": idx, "requests_made": requests_made})

    print(f"✅ Finished chunk: processed up to index {idx}/{len(ids)}; "
          f"requests this run: {requests_made}; wrote to {OUTPUT_CSV}")

if __name__ == "__main__":
    sync_metadata()
