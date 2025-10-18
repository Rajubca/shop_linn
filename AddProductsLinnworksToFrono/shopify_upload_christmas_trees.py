import os
import csv
import json
import time
import base64
import pathlib
from typing import Dict, Any, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ============ Config / .env ============
load_dotenv()

SHOPIFY_STORE_NAME   = os.getenv("SHOPIFY_STORE_NAME", "").strip()   # e.g. mystore (without .myshopify.com)
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
API_VERSION          = os.getenv("API_VERSION", "2025-01").strip()
DRY_RUN              = os.getenv("DRY_RUN", "false").strip().lower() == "true"

INPUT_CSV            = os.getenv("INPUT_CSV", "ebay_prices.csv")  # columns: SKU, Title, Price [, Description]
IMAGES_ROOT          = os.getenv("IMAGES_ROOT", r"D:\wamp64\www\MagentoProductListings\images")
COLLECTION_TITLE     = os.getenv("COLLECTION_TITLE", "Christmas Trees")
PROCESS_LIMIT        = int(os.getenv("PROCESS_LIMIT", "5"))  # process only first N rows

# Network / safety
TIMEOUT              = 40
RETRY_STATUS         = {429, 500, 502, 503, 504}
RETRY_MAX_ATTEMPTS   = 6
RETRY_BASE_DELAY_S   = 0.5

SESSION              = requests.Session()

# ============ Helpers ============
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
    time.sleep(min(RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), 8.0))

def req(method: str, path: str, params: dict = None, json_body: dict = None) -> dict:
    """REST with retries/backoff, returns JSON dict."""
    url = f"{shopify_base()}{path}"
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        resp = SESSION.request(
            method, url, headers=shopify_headers(), params=params, json=json_body, timeout=TIMEOUT
        )
        if resp.status_code in RETRY_STATUS:
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
    """GraphQL POST with retries."""
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
                raise RuntimeError(f"GQL {resp.status_code} after retries: {resp.text[:500]}")
            backoff_sleep(attempt)
            continue
        if resp.status_code >= 300:
            raise RuntimeError(f"GQL HTTP {resp.status_code}: {resp.text[:800]}")
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GQL errors: {data['errors']}")
        return data
    return {}

def load_rows(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = [c.strip() for c in (reader.fieldnames or [])]
        required = {"SKU", "Title", "Price"}
        if not required.issubset(set(fields)):
            raise ValueError(f"CSV must contain columns: {sorted(required)}. Found: {fields}")
        for r in reader:
            rows.append({
                "SKU": (r.get("SKU") or "").strip(),
                "Title": (r.get("Title") or "").strip(),
                "Price": (r.get("Price") or "").strip(),
                "Description": (r.get("Description") or "").strip(),  # optional
            })
    return rows

def _slug(s: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in (s or "").strip()).strip("-")

# ============ Shopify find/create helpers ============
def _decode_gid(gid: str) -> Optional[int]:
    # gid looks like: "gid://shopify/ProductVariant/1234567890"
    try:
        return int(gid.rsplit("/", 1)[-1])
    except Exception:
        return None

def find_variant_by_sku(sku: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Reliable SKU→(product_id, variant_id) via GraphQL:
      productVariants(first:1, query:"sku:<SKU>")
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
    edges = (((data.get("data") or {}).get("productVariants") or {}).get("edges") or [])
    if not edges:
        return None, None
    node = edges[0]["node"]
    v_gid = node["id"]
    p_gid = node["product"]["id"]
    variant_id = _decode_gid(v_gid)
    product_id = _decode_gid(p_gid)
    return product_id, variant_id

def ensure_collection(title: str) -> int:
    """Find or create a Custom Collection by title; return collection_id."""
    data = req("GET", "/custom_collections.json", params={"limit": 250})
    cols = data.get("custom_collections") or []
    for c in cols:
        if (c.get("title") or "").strip().lower() == title.strip().lower():
            return int(c["id"])

    if DRY_RUN:
        print(f"[DRY_RUN] Would create custom collection: {title}")
        return 999_000_001

    body = {"custom_collection": {"title": title, "published": True}}
    col = req("POST", "/custom_collections.json", json_body=body)
    return int(col["custom_collection"]["id"])

def attach_to_collection(product_id: int, collection_id: int):
    """Create Collect link if not already present (tolerate 422 already-exists)."""
    if DRY_RUN:
        print(f"[DRY_RUN] Would link product {product_id} to collection {collection_id}")
        return
    try:
        req("POST", "/collects.json", json_body={"collect": {"product_id": product_id, "collection_id": collection_id}})
    except RuntimeError as e:
        msg = str(e)
        if "422" in msg and ("already exists" in msg or "has already been taken" in msg):
            return
        raise

# ============ Images ============
def encode_image_base64(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

def gather_images_for_sku(sku: str) -> List[Dict[str, Any]]:
    """Returns list of product image payloads with 'attachment' base64."""
    folder = pathlib.Path(IMAGES_ROOT) / sku
    if not folder.exists():
        return []
    images = []
    pats = ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp"]
    files: List[pathlib.Path] = []
    for p in pats:
        files.extend(sorted(folder.glob(p)))
    for p in files:
        b64 = encode_image_base64(str(p))
        if b64:
            images.append({"attachment": b64, "filename": p.name})  # filename used for dedupe
    return images

def list_product_images(product_id: int) -> List[dict]:
    data = req("GET", f"/products/{product_id}/images.json")
    return data.get("images") or []

def upload_images_to_product(product_id: int, images: List[Dict[str, Any]]):
    """
    Upload only missing images; do not exceed 250 total media per product.
    Dedup by filename (best-effort). If limit reached/near, skip extras.
    """
    if not images:
        return

    existing = list_product_images(product_id)
    existing_names = { (img.get("alt") or img.get("filename") or os.path.basename(img.get("src",""))).lower()
                       for img in existing if isinstance(img, dict) }

    total = len(existing)
    if total >= 250:
        print(f"  ! Skipping images: product {product_id} already has {total} images (at Shopify limit).")
        return

    def _name_of(img_payload: dict) -> str:
        return (img_payload.get("filename") or "").lower()

    to_upload = []
    for img in images:
        name = _name_of(img)
        if name and name in existing_names:
            continue
        to_upload.append(img)

    room = max(0, 250 - total)
    to_upload = to_upload[:room]

    if not to_upload:
        print(f"  = No new images to upload for product {product_id}.")
        return

    if DRY_RUN:
        print(f"[DRY_RUN] Would upload {len(to_upload)} images to product {product_id} (room={room})")
        return

    for img in to_upload:
        body = {"image": img}
        req("POST", f"/products/{product_id}/images.json", json_body=body)
        time.sleep(0.2)

# ============ Create / Update ============
def create_product(title: str, sku: str, price: str, description: str, images: List[Dict[str, Any]]) -> int:
    """Create new product with one variant and images; returns product_id."""
    handle = _slug(sku or title)
    product_payload = {
        "product": {
            "title": title or sku,
            "handle": handle,                          # deterministic URL handle
            "body_html": description or "",
            "product_type": COLLECTION_TITLE,          # "Christmas Trees"
            "tags": [COLLECTION_TITLE],
            "status": "active",
            "variants": [
                {
                    "sku": sku,
                    "price": str(price) if price is not None else "0",
                }
            ],
        }
    }
    if images:
        product_payload["product"]["images"] = images

    if DRY_RUN:
        print(f"[DRY_RUN] Would CREATE product for SKU={sku}\n  Payload: {json.dumps(product_payload)[:400]}...")
        return 999_000_002

    resp = req("POST", "/products.json", json_body=product_payload)
    return int(resp["product"]["id"])

def update_product_and_variant(product_id: int, variant_id: int, title: str, price: str, description: str):
    """Update title/description and variant price."""
    if DRY_RUN:
        print(f"[DRY_RUN] Would UPDATE product {product_id} & variant {variant_id} (title/desc/price)")
        return
    body_p = {"product": {"id": product_id}}
    if title:
        body_p["product"]["title"] = title
    if description is not None:
        body_p["product"]["body_html"] = description
    if len(body_p["product"]) > 1:
        req("PUT", f"/products/{product_id}.json", json_body=body_p)
    if price is not None and price != "":
        body_v = {"variant": {"id": variant_id, "price": str(price)}}
        req("PUT", f"/variants/{variant_id}.json", json_body=body_v)

# ============ Main ============
def main():
    print(f"Store: {SHOPIFY_STORE_NAME}  API: {API_VERSION}  DRY_RUN={DRY_RUN}")
    rows = load_rows(INPUT_CSV)
    if PROCESS_LIMIT > 0:
        rows = rows[:PROCESS_LIMIT]
    print(f"Limiting run to {len(rows)} product(s) (PROCESS_LIMIT={PROCESS_LIMIT})")

    if not rows:
        print("No rows to process.")
        return

    collection_id = ensure_collection(COLLECTION_TITLE)
    print(f"Using collection '{COLLECTION_TITLE}' (id={collection_id})")

    processed = 0
    for row in rows:
        sku   = row["SKU"].strip()
        title = row["Title"].strip()
        price = row["Price"].strip()
        desc  = (row.get("Description") or "").strip()

        if not sku:
            continue

        images = gather_images_for_sku(sku)

        # Robust lookup via GraphQL (SKU → product_id, variant_id)
        product_id, variant_id = find_variant_by_sku(sku)

        if product_id and variant_id:
            update_product_and_variant(product_id, variant_id, title, price, desc)
            upload_images_to_product(product_id, images)
            attach_to_collection(product_id, collection_id)
            print(f"[UPDATE] SKU={sku} → product_id={product_id}, variant_id={variant_id}")
        else:
            product_id = create_product(title, sku, price, desc, images)
            attach_to_collection(product_id, collection_id)
            print(f"[CREATE] SKU={sku} → product_id={product_id}")

        processed += 1
        time.sleep(0.25)

    print(f"Done. Processed {processed} products.")

if __name__ == "__main__":
    main()
