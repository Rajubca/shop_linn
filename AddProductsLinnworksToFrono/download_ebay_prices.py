import os, time, json, requests, pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any

# ===== Config =====
INPUT_SKU_CSV  = os.getenv("INPUT_CSV", "ChristmasTree_products_sku.csv")  # must contain column 'linnworks_sku'
OUTPUT_CSV     = os.getenv("OUTPUT_CSV", "ebay_prices.csv")
TIMEOUT        = int(os.getenv("TIMEOUT", "60"))
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "80"))
REQUEST_DELAY  = float(os.getenv("REQUEST_DELAY", "0.15"))

CHANNEL_SOURCE    = (os.getenv("CHANNEL_SOURCE") or "EBAY").strip()        # e.g. EBAY
CHANNEL_SUBSOURCE = (os.getenv("CHANNEL_SUBSOURCE") or "EBAY1_UK").strip() # e.g. Ebay1_UK

# ===== Auth =====
def authorize() -> Dict[str, str]:
    app_id = os.environ["LINNWORKS_APPLICATION_ID"]
    app_secret = os.environ["LINNWORKS_APPLICATION_SECRET"]
    grant_token = os.environ["LINNWORKS_GRANT_TOKEN"]
    url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"
    r = requests.post(url, json={"applicationId": app_id, "applicationSecret": app_secret, "token": grant_token}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return {"token": data["Token"], "server": data["Server"].rstrip("/")}

def headers(token: str) -> dict:
    # Your tenant accepts raw token (no "Bearer ")
    return {"Authorization": token}

def _ensure_json(obj):
    if not isinstance(obj, str): return obj
    try:
        obj = json.loads(obj)
    except Exception:
        return obj
    if isinstance(obj, str):
        try: obj = json.loads(obj)
        except Exception: pass
    return obj

# ===== helpers (form 'request=<json>' wrapper) =====
def post_request_wrapper(url: str, hdrs: dict, payload_obj: dict):
    r = requests.post(url, headers={**hdrs, "Content-Type": "application/x-www-form-urlencoded"},
                      data={"request": json.dumps(payload_obj)}, timeout=TIMEOUT)
    try:
        r.raise_for_status()
    except Exception:
        print("ERROR:", r.text[:400])
        raise
    try:
        return r.json()
    except ValueError:
        return r.text

# ===== Endpoints =====
def get_stock_item_ids_by_sku(server: str, token: str, skus: List[str]) -> Dict[str, List[str]]:
    """Inventory/GetStockItemIdsBySKU -> { sku: [StockItemId,...] }"""
    url = f"{server}/api/Inventory/GetStockItemIdsBySKU"
    data = None
    try:
        data = post_request_wrapper(url, headers(token), {"skus": skus})
    except requests.HTTPError:
        data = post_request_wrapper(url, headers(token), {"SKUS": skus})

    data = _ensure_json(data)
    if isinstance(data, dict):
        if "Data" in data and isinstance(data["Data"], list): data = data["Data"]
        elif "Items" in data and isinstance(data["Items"], list): data = data["Items"]

    mapping: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list): mapping[str(k)] = [str(x) for x in v]
            elif isinstance(v, str): mapping[str(k)] = [v]
    elif isinstance(data, list):
        for row in data:
            if not isinstance(row, dict): continue
            sku = row.get("SKU") or row.get("Sku") or row.get("sku")
            ids = row.get("StockItemIds") or row.get("Ids") or []
            single = row.get("StockItemId") or row.get("Id")
            if single and not ids: ids = [single]
            if sku: mapping[sku] = ids if isinstance(ids, list) else [ids]
    return mapping

def get_inventory_item_prices(server: str, token: str, stock_item_id: str) -> list[dict]:
    """
    Inventory/GetInventoryItemPrices -> list of {Source, SubSource, Price, Currency, ...}
    Try GET first; if tenant requires legacy form, fallback to POST request=...
    """
    url = f"{server}/api/Inventory/GetInventoryItemPrices"
    # 1) GET
    r = requests.get(url, headers=headers(token), params={"inventoryItemId": stock_item_id}, timeout=TIMEOUT)
    if r.status_code == 200:
        try:
            return r.json()
        except ValueError:
            pass
    # 2) Fallback: legacy form wrapper
    data = post_request_wrapper(url, headers(token), {"inventoryItemId": stock_item_id})
    data = _ensure_json(data)
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for k in ("Data", "Items", "items", "Result", "result"):
            if k in data and isinstance(data[k], list):
                return data[k]
    return []

def pick_channel_price(rows: list[dict], source: str, subsource: str) -> float | None:
    """Return only the numeric price for the requested channel (no currency)."""
    for r in rows or []:
        if (r.get("Source") or "").upper() == source.upper() and (r.get("SubSource") or "") == subsource:
            return r.get("Price")
    return None

# ===== Main =====
def main():
    load_dotenv()
    auth = authorize()
    token, server = auth["token"], auth["server"]
    print("Server:", server)
    print(f"Channel: {CHANNEL_SOURCE} / {CHANNEL_SUBSOURCE}")

    df = pd.read_csv(INPUT_SKU_CSV)
    sku_col = [c for c in df.columns if c.lower().strip() == "linnworks_sku"]
    if not sku_col:
        raise ValueError("Input must contain 'linnworks_sku' column")
    skus = df[sku_col[0]].dropna().astype(str).str.strip().unique().tolist()
    print(f"Processing {len(skus)} SKUs...")

    # 1) Map SKUs -> StockItemIds
    sku_to_ids: Dict[str, List[str]] = {}
    for i in range(0, len(skus), CHUNK_SIZE):
        chunk = [s for s in skus[i:i+CHUNK_SIZE] if s]
        if not chunk: continue
        mapping = get_stock_item_ids_by_sku(server, token, chunk)
        for sku in chunk:
            if mapping.get(sku):
                sku_to_ids[sku] = mapping[sku]
        time.sleep(REQUEST_DELAY)

    # 2) Prepare fresh result map (ensures every cell is updated on each run)
    prices: Dict[str, Any] = {sku: "" for sku in skus}

    # 3) Fetch prices (use first StockItemId per SKU)
    for sku in skus:
        ids = sku_to_ids.get(sku) or []
        if not ids:
            prices[sku] = ""  # not found → blank
            continue
        sid = ids[0]
        try:
            rows = get_inventory_item_prices(server, token, sid)
            price = pick_channel_price(rows, CHANNEL_SOURCE, CHANNEL_SUBSOURCE)
            prices[sku] = price if price is not None else ""
        except Exception:
            prices[sku] = ""  # on any error, write blank
        time.sleep(REQUEST_DELAY)

    # 4) Overwrite output with fresh data (SKU, Price only)
    out_df = pd.DataFrame(
        [{"SKU": sku, "Price": prices[sku]} for sku in skus],
        columns=["SKU", "Price"]
    )
    out_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✓ Wrote {len(out_df)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
