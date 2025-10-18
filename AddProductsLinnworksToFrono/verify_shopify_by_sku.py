import os, time, requests, csv
from dotenv import load_dotenv

load_dotenv()

STORE = os.getenv("SHOPIFY_STORE_NAME", "").strip()
TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
API_VERSION = os.getenv("API_VERSION", "2025-01").strip()

INPUT_CSV = os.getenv("INPUT_CSV", "ebay_prices.csv")  # columns: SKU, Title, Price (Title/Price optional here)
COLLECTION_TITLE = os.getenv("COLLECTION_TITLE", "Christmas Trees")
AUTO_FIX_COLLECTS = os.getenv("AUTO_FIX_COLLECTS", "false").lower() == "true"
TIMEOUT = 40

BASE = f"https://{STORE}.myshopify.com/admin/api/{API_VERSION}"
HDRS = {"X-Shopify-Access-Token": TOKEN, "Accept": "application/json"}

def get_collection_id_by_title(title: str) -> int | None:
    r = requests.get(f"{BASE}/custom_collections.json", headers=HDRS, params={"limit": 250}, timeout=TIMEOUT)
    r.raise_for_status()
    for c in r.json().get("custom_collections", []):
        if (c.get("title") or "").strip().lower() == title.strip().lower():
            return int(c["id"])
    return None

def get_variant_by_sku(sku: str):
    r = requests.get(f"{BASE}/variants.json", headers=HDRS, params={"sku": sku, "limit": 1}, timeout=TIMEOUT)
    r.raise_for_status()
    arr = r.json().get("variants", [])
    return arr[0] if arr else None

def get_product(pid: int):
    r = requests.get(f"{BASE}/products/{pid}.json", headers=HDRS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("product", {})

def is_in_collection(product_id: int, collection_id: int) -> bool:
    r = requests.get(f"{BASE}/collects.json", headers=HDRS, params={"product_id": product_id, "collection_id": collection_id}, timeout=TIMEOUT)
    r.raise_for_status()
    arr = r.json().get("collects", [])
    return len(arr) > 0

def link_to_collection(product_id: int, collection_id: int):
    body = {"collect": {"product_id": product_id, "collection_id": collection_id}}
    r = requests.post(f"{BASE}/collects.json", headers={**HDRS, "Content-Type": "application/json"}, json=body, timeout=TIMEOUT)
    # 201 on success, 422 if already exists
    if r.status_code not in (200, 201):
        print(f"  ! Collect create returned {r.status_code}: {r.text[:200]}")

def load_skus(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        col = None
        for c in (reader.fieldnames or []):
            if c and c.strip().lower() == "sku":
                col = c
                break
            if c and c.strip().lower() == "linnworks_sku":
                col = c
                break
        if not col: raise ValueError("CSV must have 'SKU' or 'linnworks_sku' column.")
        return [ (row.get(col) or "").strip() for row in reader if (row.get(col) or "").strip() ]

def main():
    if not STORE or not TOKEN:
        raise SystemExit("Missing SHOPIFY_STORE_NAME or SHOPIFY_ACCESS_TOKEN in .env")

    collection_id = get_collection_id_by_title(COLLECTION_TITLE)
    if not collection_id:
        print(f"⚠ Collection '{COLLECTION_TITLE}' not found. Products can still exist but won't show in that collection.")
    else:
        print(f"✓ Using collection '{COLLECTION_TITLE}' (id={collection_id})")

    skus = load_skus(INPUT_CSV)
    print(f"Auditing {len(skus)} SKUs…\n")

    missing, found = 0, 0
    for sku in skus:
        print(f"SKU: {sku}")
        variant = get_variant_by_sku(sku)
        if not variant:
            print("  ✗ Variant not found by SKU")
            missing += 1
            print()
            continue

        vid = variant["id"]
        pid = variant["product_id"]
        price = variant.get("price")
        print(f"  ✓ Variant ID: {vid} | Product ID: {pid} | Variant price: {price}")

        product = get_product(pid)
        title = product.get("title")
        status = product.get("status")
        product_type = product.get("product_type")
        tags = product.get("tags")
        handle = product.get("handle")
        admin_url = f"https://admin.shopify.com/store/{STORE}/products/{pid}"
        online_url = f"https://{STORE}.myshopify.com/products/{handle}" if handle else "(no handle yet)"

        print(f"  Title: {title}")
        print(f"  Status: {status}  |  Product type: {product_type}  |  Tags: {tags}")
        print(f"  Admin: {admin_url}")
        print(f"  Online (might 404 if not published): {online_url}")

        if collection_id:
            linked = is_in_collection(pid, collection_id)
            print(f"  In '{COLLECTION_TITLE}' collection: {'YES' if linked else 'NO'}")
            if not linked and AUTO_FIX_COLLECTS:
                print("  -> Adding to collection…")
                link_to_collection(pid, collection_id)
                # recheck
                linked = is_in_collection(pid, collection_id)
                print(f"  In collection after fix: {'YES' if linked else 'NO'}")

        found += 1
        print()
        time.sleep(0.15)

    print(f"Done. Found: {found} | Missing: {missing}")

if __name__ == "__main__":
    main()
