import os, time, json
from typing import List, Dict
import requests
import pandas as pd
from dotenv import load_dotenv

# ---------- Config ----------
INPUT_SKU_CSV   = "ChristmasTree_products_sku.csv"  # column: linnworks_sku
OUTPUT_CSV      = "linnworks_item_details.csv"
NOT_FOUND_CSV   = "not_found_skus.csv"
BATCH_SIZE_IDS  = 80
TIMEOUT         = 60
RETRY_WAIT      = 2

# ---------- Auth ----------
def authorize() -> Dict[str, str]:
    app_id = os.environ["LINNWORKS_APPLICATION_ID"]
    app_secret = os.environ["LINNWORKS_APPLICATION_SECRET"]
    grant_token = os.environ["LINNWORKS_GRANT_TOKEN"]

    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    resp = requests.post(
        url,
        json={"applicationId": app_id, "applicationSecret": app_secret, "token": grant_token},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return {"token": data["Token"], "server": data["Server"]}

def with_auth_headers(token: str) -> Dict[str, str]:
    # Linnworks expects the raw token in Authorization
    return {"Authorization": token}

# ---------- HTTP helpers ----------
def _ensure_json(obj):
    """Handle text/plain and double-encoded JSON -> Python types."""
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

def _try_post(url, headers, *, json_body=None, form_body=None, retries=2):
    for attempt in range(1, retries + 1):
        try:
            if json_body is not None:
                r = requests.post(
                    url,
                    headers={**headers, "Content-Type": "application/json"},
                    json=json_body,
                    timeout=TIMEOUT
                )
            else:
                r = requests.post(
                    url,
                    headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
                    data=form_body,
                    timeout=TIMEOUT
                )
            if r.status_code >= 400:
                msg = f"{r.status_code} {r.reason} - {r.text[:400]}"
                if attempt < retries and r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(RETRY_WAIT * attempt)
                    continue
                raise requests.HTTPError(msg, response=r)
            # Some Linnworks endpoints return text/plain JSON strings
            try:
                return r.json()
            except ValueError:
                return r.text
        except requests.RequestException:
            if attempt >= retries:
                raise
            time.sleep(RETRY_WAIT * attempt)

def post_json(url: str, headers: Dict[str, str], payload: dict, retries=3):
    for attempt in range(1, retries + 1):
        r = requests.post(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=TIMEOUT
        )
        if r.status_code >= 400:
            body = r.text[:400]
            if attempt < retries and r.status_code in (429, 500, 502, 503, 504):
                time.sleep(RETRY_WAIT * attempt)
                continue
            raise requests.HTTPError(f"{r.status_code} {r.reason} - {body}", response=r)
        try:
            return r.json()
        except ValueError:
            return r.text

# ---------- API helpers ----------
def get_stock_item_ids_by_sku(server: str, token: str, skus: List[str]) -> Dict[str, List[str]]:
    """
    POST {server}/api/Inventory/GetStockItemIdsBySKU
    Tries JSON, then form skus=..., then form request={...}
    Returns mapping: sku -> [StockItemId, ...]
    """
    url = f"{server}/api/Inventory/GetStockItemIdsBySKU"
    hdrs = with_auth_headers(token)

    data = None
    try:
        data = _try_post(url, hdrs, json_body={"skus": skus})
    except requests.HTTPError:
        try:
            data = _try_post(url, hdrs, form_body={"skus": json.dumps(skus)})
        except requests.HTTPError:
            data = _try_post(url, hdrs, form_body={"request": json.dumps({"skus": skus})})

    data = _ensure_json(data)

    # Debug glimpse (first call only)
    print("GetStockItemIdsBySKU -> type:", type(data).__name__)

    # Some tenants return {"Data":[{...}]} or direct list/dict
    if isinstance(data, dict) and "Data" in data:
        data_list = data["Data"]
    else:
        data_list = data

    if isinstance(data_list, str):
        raise ValueError(f"Unexpected API response (string): {data_list[:300]}")

    # If dict of {SKU: [ids], ...} normalize to list of rows
    if isinstance(data_list, dict):
        normalized = [{"SKU": k, "StockItemIds": v} for k, v in data_list.items()]
    else:
        normalized = data_list or []

    mapping: Dict[str, List[str]] = {}
    for row in normalized:
        if isinstance(row, dict):
            sku = row.get("SKU") or row.get("Sku") or row.get("sku")
            ids = row.get("StockItemIds") or row.get("stockItemIds") or row.get("Ids") or []
            if sku:
                mapping[sku] = ids
        elif isinstance(row, str):
            mapping[row] = []
    return mapping

def get_inventory_items_by_ids(server: str, token: str, ids: List[str]) -> List[dict]:
    """
    POST {server}/api/Inventory/GetInventoryItemsByIds
    Tries JSON and legacy form ('request=' wrapper). Normalizes odd responses.
    """
    if not ids:
        return []

    url = f"{server}/api/Inventory/GetInventoryItemsByIds"
    headers = with_auth_headers(token)

    data = None
    # 1) JSON body: {"ids":[...]}
    try:
        data = _try_post(url, headers, json_body={"ids": ids})
    except requests.HTTPError:
        # 2) Legacy: form-urlencoded with 'request' wrapper
        data = _try_post(url, headers, form_body={"request": json.dumps({"ids": ids})})

    data = _ensure_json(data)

    # Debug glimpse (comment out later)
    print("GetInventoryItemsByIds -> type:", type(data).__name__)

    # Common shapes: list OR {"Data":[...]} OR {"Items":[...]}
    if isinstance(data, dict):
        for k in ("Data", "Items", "items", "Result", "result"):
            if k in data and isinstance(data[k], list):
                return data[k]
        # If it looks like a single item dict, wrap it
        if data.get("StockItemId") or data.get("Id"):
            return [data]
        return []

    if isinstance(data, list):
        return data

    return []


# ---------- Main ----------
def main():
    load_dotenv()
    auth = authorize()
    token, server = auth["token"], auth["server"]
    print("Server:", server)

    # Load SKUs
    df = pd.read_csv(INPUT_SKU_CSV)
    sku_col = [c for c in df.columns if c.lower().strip() == "linnworks_sku"]
    if not sku_col:
        raise ValueError("Input file must have a single column named 'linnworks_sku'")
    skus = df[sku_col[0]].dropna().astype(str).str.strip().unique().tolist()
    print(f"Found {len(skus)} SKUs to fetch.")

    # 1) SKUs -> StockItemIds
    sku_to_ids: Dict[str, List[str]] = {}
    CHUNK = 100
    for i in range(0, len(skus), CHUNK):
        chunk = skus[i:i+CHUNK]
        ids_map = get_stock_item_ids_by_sku(server, token, chunk)
        sku_to_ids.update(ids_map)
        time.sleep(0.2)

    # Track not-found SKUs (no IDs returned)
    not_found = [s for s in skus if not sku_to_ids.get(s)]

    # 2) Fetch details by IDs (batched)
    all_pairs = []
    for sku, ids in sku_to_ids.items():
        if not ids:
            all_pairs.append({"SKU": sku, "StockItemId": None})
        else:
            for sid in ids:
                all_pairs.append({"SKU": sku, "StockItemId": sid})

    all_items: List[dict] = []
    ids_only = [p["StockItemId"] for p in all_pairs if p["StockItemId"]]
    # Optional: sanity check one ID first
    if ids_only:
        sample = get_inventory_items_by_ids(server, token, ids_only[:1])
        print("SAMPLE ITEM:", (sample[0].get("ItemTitle") if sample else "no item"), "OK")

    # Fetch each batch properly
    for i in range(0, len(ids_only), BATCH_SIZE_IDS):
        batch_ids = ids_only[i:i+BATCH_SIZE_IDS]
        items = get_inventory_items_by_ids(server, token, batch_ids)
        all_items.extend(items)
        time.sleep(0.25)


    by_id = {item.get("StockItemId"): item for item in all_items}

    rows = []
    for p in all_pairs:
        sku = p["SKU"]
        sid = p["StockItemId"]
        item = by_id.get(sid, {}) if sid else {}
        rows.append({
            "SKU": sku,
            "StockItemId": sid,
            "ItemNumber": item.get("ItemNumber"),
            "Title": item.get("ItemTitle") or item.get("Title"),
            "Barcode": item.get("Barcode"),
            "RetailPrice": item.get("RetailPrice"),
            "PurchasePrice": item.get("PurchasePrice"),
            "Category": item.get("CategoryName") or item.get("Category"),
            "Weight": item.get("Weight"),
            "EAN": (item.get("EAN") or (item.get("ProductIdentifiers") or {}).get("EAN")),
            "UPC": (item.get("UPC") or (item.get("ProductIdentifiers") or {}).get("UPC")),
            "ISBN": (item.get("ISBN") or (item.get("ProductIdentifiers") or {}).get("ISBN")),
        })

    out = pd.DataFrame(rows).drop_duplicates(subset=["SKU","StockItemId"])
    out.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(out)} rows to {OUTPUT_CSV}")

    if not_found:
        pd.DataFrame({"linnworks_sku": not_found}).to_csv(NOT_FOUND_CSV, index=False)
        print(f"{len(not_found)} SKUs had no StockItemId. See {NOT_FOUND_CSV}")

if __name__ == "__main__":
    main()
