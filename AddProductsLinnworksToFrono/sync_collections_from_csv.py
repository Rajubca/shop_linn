import os
import csv
import time
from typing import Dict, Any, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ============ Load config / .env ============
load_dotenv()

SHOPIFY_STORE_NAME   = os.getenv("SHOPIFY_STORE_NAME", "").strip()
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
API_VERSION          = os.getenv("API_VERSION", "2025-01").strip()
DRY_RUN              = os.getenv("DRY_RUN", "false").strip().lower() == "true"

# This is YOUR new CSV that you generated:
# linnworks_sku,Collection1,Collection2,Collection3
INPUT_CSV            = os.getenv("CATEGORY_INPUT_CSV", "linnworks_sku_category_final.csv")

# Network / safety
TIMEOUT              = 40
RETRY_STATUS         = {429, 500, 502, 503, 504}
RETRY_MAX_ATTEMPTS   = 6
RETRY_BASE_DELAY_S   = 0.5

SESSION              = requests.Session()

# ============ Core helpers copied / reused ============

def shopify_base() -> str:
    if not SHOPIFY_STORE_NAME or not SHOPIFY_ACCESS_TOKEN:
        raise SystemExit("Missing SHOPIFY_STORE_NAME or SHOPIFY_ACCESS_TOKEN in .env")
    return f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/{API_VERSION}"

def shopify_headers() -> Dict[str, str]:
    return {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def graphql_url() -> str:
    return f"{shopify_base()}/graphql.json"

def backoff_sleep(attempt: int):
    # exponential backoff, capped
    time.sleep(min(RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), 8.0))

def req(method: str, path: str, params: dict = None, json_body: dict = None) -> dict:
    """
    Generic REST with retries/backoff. Returns JSON dict.
    """
    url = f"{shopify_base()}{path}"
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        resp = SESSION.request(
            method,
            url,
            headers=shopify_headers(),
            params=params,
            json=json_body,
            timeout=TIMEOUT
        )

        if resp.status_code in RETRY_STATUS:
            # retryable status like 429 or 503
            if attempt == RETRY_MAX_ATTEMPTS:
                raise RuntimeError(f"{resp.status_code} after retries: {resp.text[:500]}")
            backoff_sleep(attempt)
            continue

        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code} {method} {path} -> {resp.text[:800]}")

        try:
            return resp.json() if resp.text else {}
        except Exception:
            return {}
    return {}

def gql(query: str, variables: dict | None = None) -> dict:
    """
    GraphQL POST with retries/backoff.
    """
    payload = {"query": query, "variables": variables or {}}
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = graphql_url()

    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        resp = SESSION.post(url, headers=headers, json=payload, timeout=TIMEOUT)

        if resp.status_code in RETRY_STATUS:
            if attempt == RETRY_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"GQL {resp.status_code} after retries: {resp.text[:500]}"
                )
            backoff_sleep(attempt)
            continue

        if resp.status_code >= 300:
            raise RuntimeError(f"GQL HTTP {resp.status_code}: {resp.text[:800]}")

        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GQL errors: {data['errors']}")
        return data

    return {}

def _decode_gid(gid: str) -> Optional[int]:
    """
    Shopify GraphQL IDs look like:
    gid://shopify/ProductVariant/1234567890
    We just want the numeric 1234567890.
    """
    try:
        return int(gid.rsplit("/", 1)[-1])
    except Exception:
        return None

def find_variant_by_sku(sku: str) -> Tuple[Optional[int], Optional[int]]:
    """
    SKU -> (product_id, variant_id) using GraphQL search.
    """
    s = (sku or "").strip()
    if not s:
        return None, None

    query = """
    query ($q: String!) {
      productVariants(first: 1, query: $q) {
        edges {
          node {
            id
            sku
            product { id }
          }
        }
      }
    }
    """
    data = gql(query, {"q": f"sku:{s}"})

    edges = (
        ((data.get("data") or {}).get("productVariants") or {})
        .get("edges")
        or []
    )
    if not edges:
        return None, None

    node = edges[0]["node"]
    v_gid = node["id"]
    p_gid = node["product"]["id"]
    variant_id = _decode_gid(v_gid)
    product_id = _decode_gid(p_gid)
    return product_id, variant_id

def ensure_collection(title: str) -> Optional[int]:
    """
    Find or create a Custom Collection by title.
    Return collection_id (int).
    """
    clean_title = (title or "").strip()
    if not clean_title:
        return None

    # First, GET all custom collections (up to limit)
    data = req("GET", "/custom_collections.json", params={"limit": 250})
    cols = data.get("custom_collections") or []

    for c in cols:
        if (c.get("title") or "").strip().lower() == clean_title.lower():
            return int(c["id"])

    # Not found, create new
    if DRY_RUN:
        print(f"[DRY_RUN] Would create custom collection: {clean_title}")
        # Return a fake ID so downstream logic still links
        # (use a large number to avoid collision with real Shopify IDs)
        return 999_000_000

    body = {
        "custom_collection": {
            "title": clean_title,
            "published": True
        }
    }
    col = req("POST", "/custom_collections.json", json_body=body)
    return int(col["custom_collection"]["id"])

def attach_to_collection(product_id: int, collection_id: int):
    """
    Attach product to collection via /collects.json.
    Ignore "already exists" errors.
    """
    if not product_id or not collection_id:
        return

    if DRY_RUN:
        print(f"[DRY_RUN] Would link product {product_id} -> collection {collection_id}")
        return

    try:
        req(
            "POST",
            "/collects.json",
            json_body={
                "collect": {
                    "product_id": product_id,
                    "collection_id": collection_id,
                }
            },
        )
    except RuntimeError as e:
        msg = str(e)
        # Shopify returns 422 if the collect already exists
        if "422" in msg and ("already exists" in msg or "has already been taken" in msg):
            return
        raise

# ============ CSV input loader for category sync ============

def load_category_rows(csv_path: str) -> List[Dict[str, Any]]:
    """
    Read mapping CSV.

    Accepts headers in any case, e.g.:
    linnworks_sku,Collection1,Collection2,Collection3
    linnworks_sku,collection1,collection2,collection3
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Category CSV not found: {csv_path}")

    out_rows: List[Dict[str, Any]] = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        # Map lowercase -> actual header so we can read flexibly
        fieldnames = reader.fieldnames or []
        norm_map = {name.lower().strip(): name for name in fieldnames}

        required_lower = ["linnworks_sku", "collection1", "collection2", "collection3"]
        missing = [col for col in required_lower if col not in norm_map]
        if missing:
            raise ValueError(
                f"CSV missing required columns: {missing}. Found: {fieldnames}"
            )

        for r in reader:
            sku_val = (r.get(norm_map["linnworks_sku"]) or "").strip()
            c1_val  = (r.get(norm_map["collection1"]) or "").strip()
            c2_val  = (r.get(norm_map["collection2"]) or "").strip()
            c3_val  = (r.get(norm_map["collection3"]) or "").strip()

            out_rows.append(
                {
                    "sku": sku_val,
                    "c1": c1_val,
                    "c2": c2_val,
                    "c3": c3_val,
                }
            )

    return out_rows

# ============ Main sync logic ============

def sync_collections():
    """
    For each SKU row:
    1. Find product by SKU.
    2. For each non-empty collection name in c1/c2/c3:
       ensure_collection(), then attach product to it.
    3. Record result in memory list.
    4. Print progress to console.
    5. At end, write log CSV.
    """

    rows = load_category_rows(INPUT_CSV)
    print(f"Loaded {len(rows)} rows from {INPUT_CSV}")
    print(f"Store: {SHOPIFY_STORE_NAME}  API: {API_VERSION}  DRY_RUN={DRY_RUN}")
    print("Starting category sync...\n")

    report_rows = []

    for row in rows:
        sku = row["sku"]
        c_names = [row["c1"], row["c2"], row["c3"]]
        c_names_clean = [c for c in c_names if c]  # drop empty

        if not sku:
            # no SKU in row, skip
            report_rows.append(
                {
                    "linnworks_sku": sku,
                    "product_id": "",
                    "collections_linked": "|".join(c_names_clean),
                    "status": "SKIP_NO_SKU",
                }
            )
            continue

        product_id, variant_id = find_variant_by_sku(sku)

        if not product_id:
            # can't continue, product missing in Shopify
            print(f"[WARN] SKU {sku}: NOT FOUND in Shopify")
            report_rows.append(
                {
                    "linnworks_sku": sku,
                    "product_id": "",
                    "collections_linked": "|".join(c_names_clean),
                    "status": "NOT_FOUND",
                }
            )
            continue

        print(f"[OK] SKU {sku} -> product_id {product_id}, variant_id {variant_id}")
        print(f"     Collections to apply: {c_names_clean}")

        linked_ids = []

        for cname in c_names_clean:
            col_id = ensure_collection(cname)
            if col_id:
                attach_to_collection(product_id, col_id)
                linked_ids.append(f"{cname}#{col_id}")
                print(f"     Linked -> {cname} ({col_id})")
            else:
                print(f"     Skipped empty collection name for {sku}")

        report_rows.append(
            {
                "linnworks_sku": sku,
                "product_id": str(product_id),
                "collections_linked": "|".join(c_names_clean),
                "status": "OK" if linked_ids else "NO_COLLECTIONS",
            }
        )

        # tiny delay to be polite
        time.sleep(0.2)

    print("\nDone syncing collections.")
    write_report(report_rows)


def write_report(rows: List[Dict[str, Any]], out_path: str = "collection_sync_log.csv"):
    """
    Save the summary of what happened for each SKU to a CSV.
    Also print a short summary to console.
    """
    if not rows:
        print("No rows to log.")
        return

    fieldnames = ["linnworks_sku", "product_id", "collections_linked", "status"]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\n--- Sync Report ---")
    for r in rows[:10]:
        # show first ~10 for quick preview in console
        print(
            f"{r['linnworks_sku']} | product {r['product_id']} | {r['collections_linked']} | {r['status']}"
        )
    print(f"... ({len(rows)} rows total)")
    print(f"Report saved to {out_path}")


if __name__ == "__main__":
    sync_collections()
