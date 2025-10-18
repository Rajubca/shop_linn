import os, time, json, requests, pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any

# ===== Config =====
INPUT_SKU_CSV  = os.getenv("INPUT_CSV", "ChristmasTree_products_sku.csv")  # must contain column 'linnworks_sku'
OUTPUT_CSV     = os.getenv("OUTPUT_CSV", "ebay_prices.csv")
TIMEOUT        = int(os.getenv("TIMEOUT", "60"))
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", "50"))
REQUEST_DELAY  = float(os.getenv("REQUEST_DELAY", "0.15"))

CHANNEL_SOURCE    = (os.getenv("CHANNEL_SOURCE") or "EBAY").strip()
CHANNEL_SUBSOURCE = (os.getenv("CHANNEL_SUBSOURCE") or "EBAY1_UK").strip()

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
    return {"Authorization": token}

def _ensure_json(obj):
    if not isinstance(obj, str): return obj
    try: obj = json.loads(obj)
    except Exception: return obj
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

def get_titles_by_ids(server: str, token: str, ids: List[str]) -> Dict[str, str]:
    """
    Batch fetch titles.
    Primary endpoint: POST {server}/api/Stock/GetStockItemsFullByIds
    Your server expects key 'StockItemIds' (not 'ids'), often via legacy form 'request=<json>'.
    Returns: {StockItemId: Title}
    """
    if not ids:
        return {}
    out: Dict[str, str] = {}
    url = f"{server}/api/Stock/GetStockItemsFullByIds"
    hdrs = headers(token)

    def _normalize(data) -> List[dict]:
        data = _ensure_json(data)
        if isinstance(data, dict):
            for k in ("Data", "Items", "items", "Result", "result"):
                if isinstance(data.get(k), list):
                    return data[k]
            # single item dict:
            if data.get("StockItemId") or data.get("Id"):
                return [data]
            return []
        return data if isinstance(data, list) else []

    # Try permutations in order of what your tenant seems to want:
    attempts = [
        ("form_request_StockItemIds", {"headers": {**hdrs, "Content-Type": "application/x-www-form-urlencoded"},
                                       "data": {"request": json.dumps({"StockItemIds": ids})}}),
        ("form_request_ids",          {"headers": {**hdrs, "Content-Type": "application/x-www-form-urlencoded"},
                                       "data": {"request": json.dumps({"ids": ids})}}),
        ("form_StockItemIds",         {"headers": {**hdrs, "Content-Type": "application/x-www-form-urlencoded"},
                                       "data": {"StockItemIds": json.dumps(ids)}}),
        ("json_StockItemIds",         {"json": {"StockItemIds": ids}}),
        ("json_ids",                  {"json": {"ids": ids}}),
    ]

    last_error = None
    for label, kwargs in attempts:
        try:
            # choose POST style by kwargs
            if "json" in kwargs:
                r = requests.post(url, headers={**hdrs, "Content-Type": "application/json"},
                                  json=kwargs["json"], timeout=TIMEOUT)
            else:
                r = requests.post(url, **kwargs, timeout=TIMEOUT)
            if r.status_code >= 400:
                last_error = f"{label}: {r.status_code} {r.reason} - {r.text[:200]}"
                continue

            rows = _normalize(r.json() if "application/json" in r.headers.get("Content-Type","").lower() else r.text)
            for it in rows:
                if not isinstance(it, dict): 
                    continue
                sid   = it.get("StockItemId") or it.get("Id")
                title = it.get("ItemTitle") or it.get("Title") or ""
                if sid:
                    out[sid] = title
            # if we got at least one title, return
            if out:
                # print(f"GetStockItemsFullByIds via {label}: OK ({len(out)})")
                return out
        except Exception as e:
            last_error = f"{label}: {e}"

    # Fallback: GET titles per-id (slower but reliable)
    title_url = f"{server}/api/Inventory/GetInventoryItemTitles"
    for sid in ids:
        if sid in out:
            continue
        try:
            r = requests.get(title_url, headers=hdrs, params={"inventoryItemId": sid}, timeout=TIMEOUT)
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    t = (rows[0].get("Title") or rows[0].get("ItemTitle") or "").strip()
                    out[sid] = t
                else:
                    out[sid] = ""
            else:
                out[sid] = ""
        except Exception:
            out[sid] = ""
        time.sleep(REQUEST_DELAY)

    if not out and last_error:
        print("GetStockItemsFullByIds attempts failed ->", last_error)
    return out

def get_inventory_item_prices(server: str, token: str, stock_item_id: str) -> list[dict]:
    url = f"{server}/api/Inventory/GetInventoryItemPrices"
    r = requests.get(url, headers=headers(token), params={"inventoryItemId": stock_item_id}, timeout=TIMEOUT)
    if r.status_code == 200:
        try: return r.json()
        except ValueError: pass
    # fallback: legacy wrapper
    data = post_request_wrapper(url, headers(token), {"inventoryItemId": stock_item_id})
    data = _ensure_json(data)
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for k in ("Data", "Items", "items", "Result", "result"):
            if isinstance(data.get(k), list): return data[k]
    return []

def pick_channel_price(rows: list[dict], source: str, subsource: str) -> float | None:
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

    # 1) SKUs → IDs
    sku_to_ids: Dict[str, List[str]] = {}
    for i in range(0, len(skus), CHUNK_SIZE):
        chunk = [s for s in skus[i:i+CHUNK_SIZE] if s]
        if not chunk: continue
        mapping = get_stock_item_ids_by_sku(server, token, chunk)
        for sku in chunk:
            if mapping.get(sku):
                sku_to_ids[sku] = mapping[sku]
        time.sleep(REQUEST_DELAY)

    # 2) Titles in batches (Stock API), with GET fallback
    all_ids = [sid for ids in sku_to_ids.values() for sid in ids]
    id_to_title: Dict[str, str] = {}
    for i in range(0, len(all_ids), CHUNK_SIZE):
        batch_ids = all_ids[i:i+CHUNK_SIZE]
        got = get_titles_by_ids(server, token, batch_ids)
        id_to_title.update(got)
        time.sleep(REQUEST_DELAY)

    # 3) Prices per SKU (first ID)
    rows = []
    for sku in skus:
        ids = sku_to_ids.get(sku) or []
        if not ids:
            rows.append({"SKU": sku, "Title": "", "Price": ""})
            continue
        sid = ids[0]
        title = id_to_title.get(sid, "")

        try:
            price_rows = get_inventory_item_prices(server, token, sid)
            price = pick_channel_price(price_rows, CHANNEL_SOURCE, CHANNEL_SUBSOURCE)
            rows.append({"SKU": sku, "Title": title, "Price": price if price is not None else ""})
        except Exception:
            rows.append({"SKU": sku, "Title": title, "Price": ""})
        time.sleep(REQUEST_DELAY)

    # 4) Overwrite output (fresh every run)
    out_df = pd.DataFrame(rows, columns=["SKU", "Title", "Price"])
    out_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✓ Wrote {len(out_df)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
