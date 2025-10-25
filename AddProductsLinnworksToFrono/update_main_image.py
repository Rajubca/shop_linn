import os
import csv
import time
import pathlib
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# ============ Config / .env ============
load_dotenv()

# Shopify config
SHOPIFY_STORE_NAME   = os.getenv("SHOPIFY_STORE_NAME", "").strip()   # e.g. mystore (without .myshopify.com)
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "").strip()
API_VERSION          = os.getenv("API_VERSION", "2025-01").strip()

# Linnworks config
LINNWORKS_APPLICATION_ID     = os.getenv("LINNWORKS_APPLICATION_ID", "").strip()
LINNWORKS_APPLICATION_SECRET = os.getenv("LINNWORKS_APPLICATION_SECRET", "").strip()
LINNWORKS_GRANT_TOKEN        = os.getenv("LINNWORKS_GRANT_TOKEN", "").strip()

# This is your data-region base (ex: https://eu-ext.linnworks.net)
# We will still call GetImagesInBulk on this host.
LINNWORKS_SERVER_OVERRIDE    = os.getenv("LINNWORKS_SERVER_OVERRIDE", "").strip()

# Only for logging so you remember how you ran curl before
LINNWORKS_AUTH_STYLE         = os.getenv("LINNWORKS_AUTH_STYLE", "Raw").strip()

# Input SKUs
INPUT_CSV            = os.getenv("INPUT_CSV", "ebay_prices_lighting.csv")  # must contain column SKU

# Legacy local images root (not used to choose main image anymore)
IMAGES_ROOT          = os.getenv("IMAGES_ROOT", r"D:\wamp64\www\MagentoProductListings\images")

# Behavior flags / limits
PROCESS_LIMIT        = int(os.getenv("PROCESS_LIMIT", "5"))  # how many SKUs max to process
DRY_RUN              = os.getenv("DRY_RUN", "false").strip().lower() == "true"

# Networking / retry tuning
TIMEOUT              = 40
RETRY_STATUS         = {429, 500, 502, 503, 504}
RETRY_MAX_ATTEMPTS   = 6
RETRY_BASE_DELAY_S   = 0.5

SESSION              = requests.Session()

# cached short-lived Linnworks session token
_LINNWORKS_SESSION_TOKEN: Optional[str] = None


# ============ Helpers ============
def _backoff_sleep(attempt: int):
    delay = min(RETRY_BASE_DELAY_S * (2 ** (attempt - 1)), 8.0)
    time.sleep(delay)


# ============ Shopify HTTP helpers ============
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


def req(method: str, path: str, params: dict = None, json_body: dict = None) -> dict:
    """
    Shopify REST with retries/backoff, returns JSON dict.
    """
    url = f"{shopify_base()}{path}"
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        resp = SESSION.request(
            method,
            url,
            headers=shopify_headers(),
            params=params,
            json=json_body,
            timeout=TIMEOUT,
        )
        if resp.status_code in RETRY_STATUS:
            if attempt == RETRY_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"{resp.status_code} after retries: {resp.text[:500]}"
                )
            _backoff_sleep(attempt)
            continue
        if resp.status_code >= 300:
            raise RuntimeError(
                f"HTTP {resp.status_code} {method} {path} -> {resp.text[:800]}"
            )
        try:
            return resp.json() if resp.text else {}
        except Exception:
            return {}
    return {}


def gql(query: str, variables: dict | None = None) -> dict:
    """
    Shopify GraphQL with retries.
    Used to map SKU -> product_id.
    """
    payload = {
        "query": query,
        "variables": variables or {},
    }
    headers = shopify_headers()
    url = graphql_url()

    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        resp = SESSION.post(url, headers=headers, json=payload, timeout=TIMEOUT)

        if resp.status_code in RETRY_STATUS:
            if attempt == RETRY_MAX_ATTEMPTS:
                raise RuntimeError(
                    f"GQL {resp.status_code} after retries: {resp.text[:500]}"
                )
            _backoff_sleep(attempt)
            continue

        if resp.status_code >= 300:
            raise RuntimeError(
                f"GQL HTTP {resp.status_code}: {resp.text[:800]}"
            )

        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GQL errors: {data['errors']}")
        return data

    return {}


# ============ Linnworks auth + helpers ============
def linnworks_authorize() -> str:
    """
    Get a live session token from Linnworks.

    We call the global auth endpoint:
        https://api.linnworks.net/api/Auth/AuthorizeByApplication

    Body:
        {
          "ApplicationId": "...",
          "ApplicationSecret": "...",
          "Token": "..."      <-- this is your GrantToken from .env
        }

    Response includes:
        "Token": "5603f3ad-ca53-47fb-9b91-bc1863a0890d",
        "TTL": 1800,
        ...
    That "Token" is what we must send as Authorization for data calls.
    """
    global _LINNWORKS_SESSION_TOKEN
    if _LINNWORKS_SESSION_TOKEN:
        return _LINNWORKS_SESSION_TOKEN

    if (not LINNWORKS_APPLICATION_ID
        or not LINNWORKS_APPLICATION_SECRET
        or not LINNWORKS_GRANT_TOKEN):
        raise RuntimeError(
            "Missing Linnworks creds in .env "
            "(LINNWORKS_APPLICATION_ID / LINNWORKS_APPLICATION_SECRET / LINNWORKS_GRANT_TOKEN)"
        )

    auth_url = "https://api.linnworks.net/api/Auth/AuthorizeByApplication"

    payload = {
        "ApplicationId": LINNWORKS_APPLICATION_ID,
        "ApplicationSecret": LINNWORKS_APPLICATION_SECRET,
        "Token": LINNWORKS_GRANT_TOKEN,
    }

    resp = SESSION.post(
        auth_url,
        headers={
            "accept": "application/json",
            "content-type": "application/json",
        },
        json=payload,
        timeout=TIMEOUT,
    )

    if resp.status_code >= 300:
        raise RuntimeError(
            f"Linnworks Auth HTTP {resp.status_code}: {resp.text[:500]}"
        )

    data = resp.json() or {}

    # We expect "Token" to be present in that response
    session_token = (
        data.get("Token")
        or data.get("AuthorizationToken")
        or data.get("SessionToken")
    )

    if not session_token:
        raise RuntimeError(
            f"Linnworks Auth: could not find session token in response: {data}"
        )

    _LINNWORKS_SESSION_TOKEN = session_token
    return session_token


def linnworks_headers() -> Dict[str, str]:
    """
    Build headers for Inventory/GetImagesInBulk (and other Linnworks data endpoints).

    Based on your working curl, Linnworks expects:
        Authorization: <session_token>

    No 'Bearer ' prefix.
    """
    session_token = linnworks_authorize()
    return {
        "Authorization": session_token,
        "accept": "application/json",
        "content-type": "application/json",
    }


def linnworks_api_base() -> str:
    """
    Build the base URL for data calls.

    You gave: LINNWORKS_SERVER_OVERRIDE=https://eu-ext.linnworks.net
    We call data endpoints like:
        https://eu-ext.linnworks.net/api/Inventory/GetImagesInBulk
    """
    base = LINNWORKS_SERVER_OVERRIDE.rstrip("/")
    return f"{base}/api"


def get_linnworks_main_filename_from_api(sku: str) -> Optional[str]:
    """
    Call Linnworks Inventory/GetImagesInBulk for a single SKU and return
    the filename of the MAIN image (IsMain == true). If no IsMain found,
    take the first image for that SKU.

    Return e.g. "8b8b6758-2fb3-4b81-be7f-97425f35c2bb.jpg"
    or None if nothing.
    """
    url = f"{linnworks_api_base()}/Inventory/GetImagesInBulk"

    payload = {
        "request": {
            "SKUS": [sku]
        }
    }

    resp = SESSION.post(
        url,
        headers=linnworks_headers(),
        json=payload,
        timeout=TIMEOUT,
    )

    if resp.status_code == 401:
        raise RuntimeError(
            "Linnworks 401 Unauthorized when calling GetImagesInBulk. "
            "Session token may have expired early, or creds are invalid."
        )
    if resp.status_code >= 300:
        raise RuntimeError(f"Linnworks HTTP {resp.status_code}: {resp.text[:500]}")

    data = resp.json() or {}
    images = data.get("Images") or []

    # Prefer IsMain == True
    for img in images:
        if img.get("SKU") == sku and img.get("IsMain") is True:
            full_src = (img.get("FullSource") or "").strip()
            if not full_src:
                continue
            filename = os.path.basename(full_src.split("?")[0]).lower()
            return filename

    # Fallback: first available for this SKU
    for img in images:
        if img.get("SKU") == sku:
            full_src = (img.get("FullSource") or "").strip()
            if not full_src:
                continue
            filename = os.path.basename(full_src.split("?")[0]).lower()
            return filename

    return None


# ============ CSV helpers ============
def load_skus(csv_path: str) -> List[str]:
    """
    Read SKUs from INPUT_CSV.
    We only care about the 'SKU' column.
    Dedupes while keeping first-seen order.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    out: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "SKU" not in [c.strip() for c in reader.fieldnames]:
            raise ValueError("CSV must contain column 'SKU'")
        for r in reader:
            sku = (r.get("SKU") or "").strip()
            if sku:
                out.append(sku)

    # remove duplicates keeping first occurrence
    seen = set()
    deduped: List[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


# ============ (legacy) local debug helper ============
def _list_local_image_files_for_sku(sku: str) -> List[pathlib.Path]:
    """
    Return all local image files for that SKU from IMAGES_ROOT/<SKU>/,
    sorted alphabetically by filename.
    (not used in the sync logic, just handy for manual checks)
    """
    folder = pathlib.Path(IMAGES_ROOT) / sku
    if not folder.exists():
        return []
    pats = ["*.jpg", "*.jpeg", "*.png", "*.gif", "*.webp"]
    files: List[pathlib.Path] = []
    for p in pats:
        files.extend(folder.glob(p))
    return sorted(files, key=lambda x: x.name.lower())


# ============ Shopify product + images helpers ============
def _decode_gid(gid: str) -> Optional[int]:
    # Shopify returns GIDs like "gid://shopify/ProductVariant/1234567890"
    try:
        return int(gid.rsplit("/", 1)[-1])
    except Exception:
        return None


def find_product_id_by_sku(sku: str) -> Optional[int]:
    """
    Use Shopify GraphQL to find the product_id associated
    with the variant that has this SKU.
    """
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
    data = gql(query, {"q": f"sku:{sku}"})
    edges = (((data.get("data") or {}).get("productVariants") or {}).get("edges") or [])
    if not edges:
        return None

    node = edges[0]["node"]
    p_gid = node["product"]["id"]
    product_id = _decode_gid(p_gid)
    return product_id


def list_product_images(product_id: int) -> List[dict]:
    """
    GET /products/{product_id}/images.json
    Returns Shopify's product images, each with id / position / src / etc.
    """
    data = req("GET", f"/products/{product_id}/images.json")
    return data.get("images") or []


def infer_shopify_filename_from_src(src: str) -> str:
    """
    Shopify CDN URL ends like ".../filename.jpg?v=1761299300".
    We take the basename before '?', lowercase.
    """
    tail = os.path.basename(src or "")
    tail = tail.split("?")[0]
    return tail.lower().strip()


def set_image_position_1(product_id: int, image_id: int, filename: str):
    """
    PUT /products/{product_id}/images/{image_id}.json
    Move that image to position 1 in Shopify.
    Honours DRY_RUN so you can test safely.
    """
    if DRY_RUN:
        print(f"[DRY_RUN] Would set product {product_id} image {image_id} ('{filename}') to position 1")
        return

    body = {
        "image": {
            "id": image_id,
            "position": 1
        }
    }
    print(f"[DEBUG] PUT /products/{product_id}/images/{image_id}.json body={body}")
    resp = req("PUT", f"/products/{product_id}/images/{image_id}.json", json_body=body)
    print(f"[DEBUG] Shopify PUT response keys: {list(resp.keys())}")


# ============ Main sync logic ============
def fix_main_image_for_sku(sku: str) -> None:
    """
    Steps for one SKU:
      1. Ask Linnworks which image is main (IsMain == true).
      2. Match that filename against Shopify's product images.
      3. If found and not already position 1, reorder it to position 1.
    """

    # 1. Linnworks main filename
    desired_main = get_linnworks_main_filename_from_api(sku)
    if not desired_main:
        print(f"[SKIP] {sku}: Linnworks didn't return a main image.")
        return

    desired_main = desired_main.lower().strip()
    print(f"[DEBUG] SKU {sku} Linnworks main filename = '{desired_main}'")

    # 2. Shopify product
    product_id = find_product_id_by_sku(sku)
    if not product_id:
        print(f"[SKIP] {sku}: No Shopify product with this SKU.")
        return
    print(f"[DEBUG] SKU {sku} Shopify product_id = {product_id}")

    # 3. Shopify images
    shop_imgs = list_product_images(product_id)
    if not shop_imgs:
        print(f"[SKIP] {sku}: Shopify product {product_id} has no images.")
        return

    print(f"[DEBUG] SKU {sku} Shopify images BEFORE reorder:")
    match_id = None
    already_main = False

    for img in shop_imgs:
        img_id = img.get("id")
        pos    = img.get("position")
        src    = img.get("src", "")
        shop_name = infer_shopify_filename_from_src(src)
        print(f"         - id={img_id} pos={pos} file={shop_name}")

        if shop_name == desired_main:
            match_id = img_id
            if pos == 1:
                already_main = True

    # 4. No filename match in Shopify
    if not match_id:
        print(f"[NO MATCH] {sku}: Shopify doesn't have Linnworks main '{desired_main}' for product {product_id}. No change.")
        return

    # 5. Already first? done
    if already_main:
        print(f"[OK] {sku}: '{desired_main}' is ALREADY position 1 for product {product_id}. No change needed.")
        return

    # 6. Promote that image to position 1
    print(f"[DEBUG] SKU {sku} → moving image_id={match_id} ('{desired_main}') to position 1 on Shopify product {product_id}")
    set_image_position_1(product_id, match_id, desired_main)

    # 7. Re-fetch after update for confirmation
    updated_imgs = list_product_images(product_id)
    print(f"[DEBUG] SKU {sku} Shopify images AFTER reorder:")
    for img in updated_imgs:
        img_id = img.get("id")
        pos    = img.get("position")
        src    = img.get("src", "")
        shop_name = infer_shopify_filename_from_src(src)
        print(f"         - id={img_id} pos={pos} file={shop_name}")

    print(f"[FIXED] {sku}: set '{desired_main}' as main image for product {product_id} (image {match_id}).")


# ============ Entrypoint ============
def main():
    print(f"Store: {SHOPIFY_STORE_NAME}  API: {API_VERSION}  DRY_RUN={DRY_RUN}")
    print(f"Linnworks data server: {LINNWORKS_SERVER_OVERRIDE}")
    print("Authorizing with Linnworks global auth endpoint (https://api.linnworks.net)...")

    # Do auth up front so we fail fast if creds are wrong
    token_preview = linnworks_authorize()
    print(f"Linnworks auth OK. Session token starts with {token_preview[:8]!r} (TTL ~30min)")

    skus = load_skus(INPUT_CSV)

    if PROCESS_LIMIT > 0:
        skus = skus[:PROCESS_LIMIT]

    print(f"Checking {len(skus)} SKU(s) (PROCESS_LIMIT={PROCESS_LIMIT})")

    count = 0
    for sku in skus:
        fix_main_image_for_sku(sku)
        count += 1
        # polite pacing for rate limits
        time.sleep(0.5)

    print(f"Done. Checked {count} SKUs.")


if __name__ == "__main__":
    main()
