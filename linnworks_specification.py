#!/usr/bin/env python3
"""
Fetch Linnworks channel descriptions for Source=EBAY, SubSource=EBAY0_UK
for a list of SKUs from linnworks_skus.csv.

- Loads .env (ENV_PATH optional).
- Auth via AuthorizeByApplication -> gets (Token, Server)
- Chooses working auth header style automatically (Bearer -> Raw fallback)
- Maps SKUs to StockItemId via Inventory/GetStockItemIdsBySKU
- Fetches descriptions via Inventory/GetInventoryItemDescriptions (GET)
- Outputs ebay_uk_descriptions.csv

.env example (no quotes/semicolons):
LINNWORKS_APPLICATION_ID=xxxx
LINNWORKS_APPLICATION_SECRET=xxxx
LINNWORKS_GRANT_TOKEN=xxxx
# Optional overrides:
# INPUT_CSV=linnworks_skus.csv
# OUTPUT_CSV=ebay_uk_descriptions.csv
# REQUEST_DELAY=0.15
# CHANNEL_SOURCE=EBAY
# CHANNEL_SUBSOURCE=EBAY0_UK
"""

import os, time, json, csv, requests
from typing import Dict, Any, List, Tuple
from dotenv import load_dotenv, find_dotenv

# ---------- .env ----------
ENV_PATH = os.getenv("ENV_PATH")
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    load_dotenv(find_dotenv(usecwd=True))

APP_ID      = (os.getenv("LINNWORKS_APPLICATION_ID") or "").strip()
APP_SECRET  = (os.getenv("LINNWORKS_APPLICATION_SECRET") or "").strip()
GRANT_TOKEN = (os.getenv("LINNWORKS_GRANT_TOKEN") or "").strip()

INPUT_CSV   = os.getenv("INPUT_CSV", "linnworks_skus.csv")
OUTPUT_CSV  = os.getenv("OUTPUT_CSV", "ebay_uk_descriptions.csv")
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.15"))

CHANNEL_SOURCE    = (os.getenv("CHANNEL_SOURCE") or "EBAY").strip()
CHANNEL_SUBSOURCE = (os.getenv("CHANNEL_SUBSOURCE") or "EBAY0_UK").strip()

# ---------- Auth ----------
def authorize_by_application() -> Tuple[str, str]:
    """Call api.linnworks.net to obtain (Token, Server)."""
    if not (APP_ID and APP_SECRET and GRANT_TOKEN):
        raise SystemExit("Missing .env vars: LINNWORKS_APPLICATION_ID / _SECRET / _GRANT_TOKEN")
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    payload = {"ApplicationId": APP_ID, "ApplicationSecret": APP_SECRET, "Token": GRANT_TOKEN}
    r = requests.post(url, json=payload, timeout=40)
    r.raise_for_status()
    data = r.json()
    token  = data.get("Token")
    server = (data.get("Server") or data.get("ServerAddress") or data.get("ServerUrl") or "").rstrip("/")
    if not token or not server:
        raise RuntimeError(f"Auth response missing Token/Server: {data}")
    return token, server

def _make_session(auth_header_value: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": auth_header_value,  # "Bearer <token>" or "<token>"
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    return s

def _probe(session: requests.Session, server: str) -> bool:
    """Cheap auth probe."""
    url = f"{server}/api/Inventory/GetChannels"
    r = session.get(url, timeout=20)
    return r.status_code == 200

def get_authed_session_and_server() -> Tuple[requests.Session, str, str, str]:
    """
    Returns (session, server, token, auth_style).
    Tries Bearer -> reauth -> Raw.
    """
    token, server = authorize_by_application()
    sess = _make_session(f"Bearer {token}")
    if _probe(sess, server):
        return sess, server, token, "Bearer"

    # re-auth once (token may be very short-lived)
    token2, server2 = authorize_by_application()
    sess2 = _make_session(f"Bearer {token2}")
    if _probe(sess2, server2):
        return sess2, server2, token2, "Bearer"

    # legacy raw style
    sess3 = _make_session(token2)
    if _probe(sess3, server2):
        return sess3, server2, token2, "Raw"

    raise SystemExit("Auth failed: token rejected with both 'Bearer' and raw header styles.")

# ---------- HTTP helpers ----------
def post_json(session: requests.Session, server: str, path: str, payload: Dict[str, Any]) -> Any:
    url = f"{server}/api{path}" if not server.endswith("/api") else f"{server}{path}"
    r = session.post(url, data=json.dumps(payload), timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} on {path}: {r.text[:500]}")
    return r.json()

def get_json(session: requests.Session, server: str, path: str, params: Dict[str, Any]) -> Any:
    url = f"{server}/api{path}" if not server.endswith("/api") else f"{server}{path}"
    r = session.get(url, params=params, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} on {path}: {r.text[:500]}")
    return r.json()

# ---------- Inventory helpers ----------
def get_stockitem_ids_by_sku(session: requests.Session, server: str, skus: List[str]) -> Dict[str, str]:
    """
    Inventory/GetStockItemIdsBySKU -> {SKU: StockItemId}
    Try JSON first, fall back to x-www-form-urlencoded "request=<json>" if needed.
    """
    # 1) JSON body
    try:
        resp = post_json(session, server, "/Inventory/GetStockItemIdsBySKU", {"request": {"SKUS": skus}})
        mapping = {}
        for it in (resp or {}).get("Items", []):
            sku, sid = it.get("SKU"), it.get("StockItemId")
            if sku and sid:
                mapping[sku] = sid
        return mapping
    except RuntimeError as e:
        if "401" not in str(e) and "415" not in str(e) and "Unsupported" not in str(e):
            raise

    # 2) Fallback content-type
    url = f"{server}/api/Inventory/GetStockItemIdsBySKU"
    body = {"request": json.dumps({"SKUS": skus})}
    headers = dict(session.headers)
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    r = session.post(url, data=body, headers=headers, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} on /Inventory/GetStockItemIdsBySKU (fallback): {r.text[:500]}")
    data = r.json()
    mapping = {}
    for it in (data or {}).get("Items", []):
        sku, sid = it.get("SKU"), it.get("StockItemId")
        if sku and sid:
            mapping[sku] = sid
    return mapping

def get_item_descriptions(session: requests.Session, server: str, stock_item_id: str) -> List[Dict[str, Any]]:
    """GET Inventory/GetInventoryItemDescriptions -> list of dicts with Source, SubSource, Description"""
    return get_json(session, server, "/Inventory/GetInventoryItemDescriptions", {"inventoryItemId": stock_item_id})

def pick_channel_description(desc_rows: List[Dict[str, Any]], source: str, subsource: str) -> str:
    for d in desc_rows or []:
        if (d.get("Source") or "").upper() == source.upper() and (d.get("SubSource") or "") == subsource:
            return d.get("Description") or ""
    return "Not Found"

# ---------- Utils ----------
def read_skus_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "linnworks_sku" not in reader.fieldnames:
            raise ValueError("CSV must contain 'linnworks_sku' column")
        return [row["linnworks_sku"].strip() for row in reader if row.get("linnworks_sku", "").strip()]

def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# ---------- Main ----------
def main():
    session, server, token, style = get_authed_session_and_server()
    print(f"[DEBUG] Using {style} auth on {server}")
    print(f"[DEBUG] Descriptions endpoint: {server}/api/Inventory/GetInventoryItemDescriptions")

    skus = read_skus_from_csv(INPUT_CSV)
    print(f"✓ Loaded {len(skus)} SKUs")

    # Map SKUs -> StockItemId
    print("Mapping SKUs → StockItemId…")
    sku_to_id: Dict[str, str] = {}
    for batch in chunked(skus, 80):
        sku_to_id.update(get_stockitem_ids_by_sku(session, server, batch))
        time.sleep(REQUEST_DELAY)
    print(f"✓ Resolved {len(sku_to_id)} items")

    missing = [s for s in skus if s not in sku_to_id]
    if missing:
        print(f"⚠ Missing {len(missing)} SKUs (not found): {missing[:10]}{' …' if len(missing) > 10 else ''}")

    # Fetch EBAY0_UK descriptions
    print(f"Fetching channel descriptions for {CHANNEL_SOURCE}/{CHANNEL_SUBSOURCE}…")
    out_rows: List[Tuple[str, str]] = []
    for idx, sku in enumerate(skus, start=1):
        sid = sku_to_id.get(sku)
        if not sid:
            out_rows.append((sku, "Not Found"))
            print(f"[{idx}] {sku}: NOT FOUND")
            continue
        try:
            desc_rows = get_item_descriptions(session, server, sid)
            desc_text = pick_channel_description(desc_rows, CHANNEL_SOURCE, CHANNEL_SUBSOURCE)
            out_rows.append((sku, desc_text))
            print(f"[{idx}] {sku}: OK")
        except Exception as e:
            out_rows.append((sku, f"Error: {e}"))
            print(f"[{idx}] {sku}: ERROR -> {e}")
        time.sleep(REQUEST_DELAY)

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["linnworks_sku", "ebay_uk_description"])
        w.writerows(out_rows)

    found = sum(1 for _, d in out_rows if d and not d.startswith("Error") and d != "Not Found")
    nf    = sum(1 for _, d in out_rows if d == "Not Found")
    errs  = sum(1 for _, d in out_rows if d.startswith("Error"))
    print(f"\n✓ Wrote {OUTPUT_CSV} | Found: {found} | Not found: {nf} | Errors: {errs}")

if __name__ == "__main__":
    main()
