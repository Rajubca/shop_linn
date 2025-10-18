import os
import time
import re
import pandas as pd
import requests
import json
from pathlib import Path
from io import StringIO
from typing import Optional

# ====== CONFIG - OLLAMA & FILE PATHS ======
INPUT_FILE   = r"D:\RPrajapati\Linnworks\ebay_uk_descriptions.csv"
INPUT_FILE   = r"D:\RPrajapati\Linnworks\TEST_5_ROWS_OLLAMA_OUTPUT_v2.csv"
DESC_COL     = "ebay_uk_description"   # Column containing the text to send to the model
OUT_COL      = "key points"            # Column where the extracted specs will be saved
SKU_COL      = "linnworks sku"         # For logging progress

OLLAMA_API_URL = "http://localhost:11434/api/generate"
MODEL_NAME   = "mistral"               # Must be the name of the model you have pulled in Ollama
SAVE_EVERY   = 1                       # Save after every row for testing

# 🎯 UPDATED INSTRUCTION
INSTRUCTION = (
    "Generate Specifications from Description. With no details loss\n"
    "Rules:\n"
    "• The entire output MUST be preceded by the heading 'Specifications'.\n"
    "• DO NOT use bullet points (•, -, *, etc.). Use a new line for each specification.\n"
    "• Use concise 'Key: Value' style where possible (e.g., 'Color: Black').\n"
    "• Keep numbers/units exact; no marketing text; no headings/titles other than 'Specifications'; no duplicates.\n"
    "• If nothing meaningful is found, return an empty string.\n"
    "Verify the output once again for key:value\n"
    
)


# ====== UTILITIES: FILE HANDLING ======
def load_csv_robust(path: Path, *, sep=None, keep_default_na=False) -> pd.DataFrame:
    """Robustly loads a CSV file by trying multiple encodings."""
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1", "iso-8859-1"]
    for enc in encodings:
        try:
            return pd.read_csv(
                path,
                dtype=str,
                encoding=enc,
                sep=sep or ",",
                engine="python",
                on_bad_lines="skip",
                keep_default_na=keep_default_na
            )
        except UnicodeDecodeError:
            continue
    with open(path, "rb") as f:
        raw = f.read().replace(b"\x00", b"")
    text = raw.decode("latin1", errors="replace")
    return pd.read_csv(StringIO(text), dtype=str, sep=sep or ",", engine="python")

def html_to_text(s: str) -> str:
    """
    Convert HTML to clean text. Tries BeautifulSoup if available; falls back to regex.
    """
    if not s:
        return ""
    try:
        # Use BeautifulSoup if installed
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(s, "html.parser")
        # Keep line breaks for <br> and <li> to help model
        for br in soup.find_all(["br", "p", "li"]):
            br.append("\n")
        text = soup.get_text(separator=" ")
    except Exception:
        # Fallback: brute-force strip tags
        text = re.sub(r"<\s*br\s*/?>", "\n", s, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()

# ====== OLLAMA API + RETRY ======
def get_response_from_ollama(prompt: str, *, max_retries: int = 3, timeout_sec: int = 180) -> str:
    """Sends a prompt to the local Ollama API and retrieves the generated response with retries."""
    headers = {'Content-Type': 'application/json'}
    data = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.1
        }
    }
    backoff = 2
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                OLLAMA_API_URL,
                headers=headers,
                data=json.dumps(data),
                timeout=timeout_sec
            )
            resp.raise_for_status()
            result = resp.json()
            return (result.get("response") or "").strip()
        except requests.exceptions.RequestException as e:
            print(f"\n\033[91m  -> Ollama request failed (attempt {attempt}/{max_retries}): {e}\033[0m")
            if attempt < max_retries:
                time.sleep(backoff)
                backoff *= 2
            else:
                print("\033[91m  -> Ensure Ollama is running (`ollama serve`) and the model is pulled (`ollama pull mistral`).\033[0m")
                return "⚠️ OLLAMA_CONNECTION_ERROR"

# ====== OUTPUT NORMALIZATION ======
def normalize_spec_output(raw: str) -> str:
    """
    Force the exact format:
      - Begins with 'Specifications' (alone on first line)
      - Then Key: Value per line
      - No bullets or extra headings
    If nothing meaningful, return empty string.
    """
    if not raw or raw == "⚠️ OLLAMA_CONNECTION_ERROR":
        return raw

    text = raw.strip()

    # Remove any markdown bullets, numbering, and excessive headings
    lines = [re.sub(r"^\s*([•\-\*\d]+\s*[\.\)])\s*", "", ln).strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]  # drop empty

    # Find start index: prefer a line exactly 'Specifications', otherwise keep from top
    start_idx = 0
    for idx, ln in enumerate(lines):
        if ln.lower() == "specifications":
            start_idx = idx
            break

    lines = lines[start_idx:]

    # Ensure first line == 'Specifications' or return empty if nothing else
    if not lines:
        return ""
    # if lines[0].lower() != "specifications":
    #     lines.insert(0, "Specifications")

    # Keep only plausible "Key: Value" lines after the header
    cleaned = ["Specifications"]
    for ln in lines[1:]:
        # If the model added extra headings or sentences, try to transform into Key: Value when possible
        # Accept pattern with a colon
        if ":" in ln:
            # Normalize "Key : Value" -> "Key: Value"
            ln = re.sub(r"\s*:\s*", ": ", ln, count=1)
            # Drop duplicates & overlong marketing lines (heuristic: > 200 chars)
            if len(ln) <= 200 and not any(ln.lower() == x.lower() for x in cleaned[1:]):
                cleaned.append(ln)
        else:
            # Try to convert simple "Key Value" into "Key: Value" if it looks like two parts
            m = re.match(r"^([A-Za-z][A-Za-z0-9 \-/&\(\)%]+)\s{1,}([^\:]{1}.*)$", ln)
            if m:
                candidate = f"{m.group(1).strip()}: {m.group(2).strip()}"
                if len(candidate) <= 200 and not any(candidate.lower() == x.lower() for x in cleaned[1:]):
                    cleaned.append(candidate)

    # If we only have the heading and no lines, return empty (as requested)
    if len(cleaned) == 1:
        return ""

    # Final pass: remove any trailing punctuation-only lines or duplicates
    unique = []
    seen = set()
    for ln in cleaned:
        k = ln.strip().lower()
        if k not in seen and ln.strip() not in {"-", "—", "•"}:
            unique.append(ln)
            seen.add(k)

    return "\n".join(unique)

# ====== MAIN EXECUTION ======
def main():
    in_path = Path(INPUT_FILE)
    if not in_path.exists():
        raise SystemExit(f"File not found: {in_path}")

    df = load_csv_robust(in_path, sep=",")

    # Ensure output column exists
    if OUT_COL not in df.columns:
        df[OUT_COL] = ""

    print(f"\nLoaded {len(df)} rows from {in_path.name}\n")

    # Set test limit
    MAX_ROWS_TO_PROCESS = 590  # adjust later
    processed_count = 0

    try:
        for i, row in df.iterrows():
            if processed_count >= MAX_ROWS_TO_PROCESS:
                print(f"\n🛑 Reached limit of {MAX_ROWS_TO_PROCESS} rows for test run.")
                break

            desc_html = (row.get(DESC_COL) or "").strip()
            if not desc_html:
                continue

            # Skip already-processed rows unless it was an error marker
            existing = str(row.get(OUT_COL) or "").strip()
            if existing and existing != "⚠️ OLLAMA_CONNECTION_ERROR":
                continue

            sku = (row.get(SKU_COL) or "").strip()
            prefix = f"Row {i+2}" + (f" | {sku}" if sku else "")
            print(f"\033[96m{prefix} → cleaning input & sending prompt…\033[0m")

            # Clean HTML before sending to the model (much better results)
            desc_text = desc_html
            # desc_text = html_to_text(desc_html)

            # Build prompt and call model
            prompt =   desc_text + INSTRUCTION
            reply = get_response_from_ollama(prompt)

            # Sanitize/normalize to guarantee required format
            final_out = normalize_spec_output(reply)

            # Store result
            df.at[i, OUT_COL] = final_out

            processed_count += 1

            # Save checkpoint
            if (processed_count % SAVE_EVERY) == 0:
                tmp_out = in_path.with_name("TEST_5_ROWS_OLLAMA_OUTPUT_v2.csv")
                df.to_csv(tmp_out, index=False, encoding="utf-8-sig")
                print(f"💾 Checkpoint saved to: {tmp_out.name}")

        # Final save
        final_out_path = in_path.with_name("TEST_5_ROWS_OLLAMA_OUTPUT_v2.csv")
        df.to_csv(final_out_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ Processing complete! Results saved to: {final_out_path.name}\n")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Attempting final save of partial data...")
        df.to_csv(in_path.with_name("partial_test_output.csv"), index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
