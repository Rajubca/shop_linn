import os, time, json, requests, pandas as pd
from dotenv import load_dotenv
from typing import List, Dict

INPUT_SKU_CSV = "ChristmasTree_products_sku.csv"
FOUND_CSV = "found_skus.csv"
NOT_FOUND_CSV = "not_found_skus.csv"
TIMEOUT = 60
RETRY_WAIT = 2

# ---------- Auth ----------
def authorize() -> Dict[str, str]:
    app_id = os.environ["LINNWORKS_APPLICATION_ID"]
    app_secret = os.environ["LINNWORKS_APPLICATION_SECRET"]
    grant_token = os.environ["LINNWORKS_GRANT_TOKEN"]

    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    r = requests.post(url, json={
        "applicationId": app_id,
        "applicationSecret": app_secret,
        "token": grant_token
    }, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return {"token": data["Token"], "server": data["Server"]}

def with_auth_headers(token: str) -> dict:
    # Your tenant works with the raw token (not "Bearer ")
    return {"Authorization": token}

# ---------- Helpers ----------
def _ensure_json(obj):
    # Handle text/plain or double-encoded JSON
    if not isinstance(obj, str): 
        return obj
    try:
        obj = json.loads(obj)
    except Exception:
        return obj
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            pass
    return obj

def _post_request_wrapper(url: str, headers: dict, payload_obj: dict):
    """
    Send as application/x-www-form-urlencoded with legacy 'request=<json>' wrapper.
    This is what your server requires for Inventory/GetStockItemIdsBySKU.
    """
    body = {"request": json.dumps(payload_obj)}
    r = requests.post(
        url,
        headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
        data=body,
        timeout=TIMEOUT
    )
    try:
        r.raise_for_status()
    except Exception:
        print("Error:", r.text[:400])
        raise
    try:
        return r.json()
    except ValueError:
        return r.text

# ---------- API ----------
def get_stock_item_ids_by_sku(server: str, token: str, skus: List[str]) -> Dict[str, List[str]]:
    """
    POST {server}/api/Inventory/GetStockItemIdsBySKU
    Force legacy 'request=<json>' form body.
    Accept both 'skus' and 'SKUS' (casing differs across tenants).
    Normalize list/dict response shapes to: { sku: [id, ...] }.
    """
    url = f"{server}/api/Inventory/GetStockItemIdsBySKU"
    hdrs = with_auth_headers(token)

    # Clean blanks
    skus = [s for s in (s.strip() for s in skus) if s]

    # Try 'skus' casing first, then 'SKUS'
    data = None
    try:
        data = _post_request_wrapper(url, hdrs, {"skus": skus})
    except requests.HTTPError:
        data = _post_request_wrapper(url, hdrs, {"SKUS": skus})

    data = _ensure_json(data)

    # Some tenants wrap in {"Data":[...]} or {"Items":[...]}
    if isinstance(data, dict):
        if "Data" in data and isinstance(data["Data"], list):
            data = data["Data"]
        elif "Items" in data and isinstance(data["Items"], list):
            data = data["Items"]

    # Normalize to mapping
    mapping: Dict[str, List[str]] = {}

    if isinstance(data, dict):
        # e.g. {"SKU1":["guid1"], "SKU2":["guid2","guid3"]}
        for k, v in data.items():
            if isinstance(v, list):
                mapping[str(k)] = [str(x) for x in v]
            elif isinstance(v, str):
                mapping[str(k)] = [v]
        return mapping

    if isinstance(data, list):
        # e.g. [{"SKU":"ABC","StockItemIds":["guid1","guid2"]}, ...]
        for row in data:
            if not isinstance(row, dict):
                continue
            sku = row.get("SKU") or row.get("Sku") or row.get("sku")
            ids = row.get("StockItemIds") or row.get("Ids") or []
            # single-id variants
            single = row.get("StockItemId") or row.get("StockItemID") or row.get("Id") or row.get("ID")
            if single and not ids:
                ids = [single]
            if sku:
                mapping[sku] = ids if isinstance(ids, list) else [ids]
        return mapping

    # Unknown shape => none found
    return {}

# ---------- MAIN ----------
def main():
    load_dotenv()
    auth = authorize()
    token, server = auth["token"], auth["server"]
    print("Server:", server)

    df = pd.read_csv(INPUT_SKU_CSV)
    sku_col = [c for c in df.columns if c.lower().strip() == "linnworks_sku"]
    if not sku_col:
        raise ValueError("No 'linnworks_sku' column found.")
    skus = df[sku_col[0]].dropna().astype(str).str.strip().unique().tolist()
    print(f"Checking {len(skus)} SKUs...")

    found, not_found = [], []
    CHUNK = 100
    first = True
    for i in range(0, len(skus), CHUNK):
        chunk = skus[i:i+CHUNK]
        mapping = get_stock_item_ids_by_sku(server, token, chunk)

        if first:
            # quick sanity peek
            key = next(iter(mapping.keys()), None)
            print("MAPPING SAMPLE:", key, "->", mapping.get(key))
            first = False

        for sku in chunk:
            if mapping.get(sku):
                found.append(sku)
            else:
                not_found.append(sku)
        time.sleep(0.2)

    pd.DataFrame({"linnworks_sku": found}).to_csv(FOUND_CSV, index=False)
    pd.DataFrame({"linnworks_sku": not_found}).to_csv(NOT_FOUND_CSV, index=False)
    print(f"✅ Found: {len(found)} | ❌ Not Found: {len(not_found)}")
    print(f"Saved to {FOUND_CSV} and {NOT_FOUND_CSV}")

if __name__ == "__main__":
    main()
