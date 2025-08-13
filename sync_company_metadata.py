# sync_company_metadata.py
import os, csv, time, json, math, random, signal
from pathlib import Path
from datetime import datetime, timezone
import httpx

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

def load_ids():
    # Expect a CSV with a header and a column named 'company_id' or 'companies_house_registered_number'
    # or a simple one-column CSV.
    ids = []
    with open(INPUT_NEW_IDS_CSV, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        if "company_id" in rdr.fieldnames:
            ids = [r["company_id"].strip() for r in rdr if r.get("company_id")]
        elif "companies_house_registered_number" in rdr.fieldnames:
            ids = [r["companies_house_registered_number"].strip() for r in rdr if r.get("companies_house_registered_number")]
        else:
            # fallback: first column
            f.seek(0)
            simple = csv.reader(f)
            for row in simple:
                if not row: continue
                val = row[0].strip()
                if val.lower() in ("company_id", "companies_house_registered_number"):  # skip header
                    continue
                ids.append(val)
    # de-dupe & keep order
    seen = set(); out=[]
    for cid in ids:
        if cid and cid not in seen:
            seen.add(cid); out.append(cid)
    return out

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
