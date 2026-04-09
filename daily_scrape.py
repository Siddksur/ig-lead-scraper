"""
daily_scrape.py — Daily Instagram lead scraper for Canadian real estate agents.
Runs via GitHub Actions at 9am EST.
Pipeline: Apify → clean names → Google Sheets → GoHighLevel

City rotation: scrapes one city at a time until 500 leads are collected,
then automatically moves to the next city.
Cities: Toronto → Vancouver → Calgary → Edmonton → Mississauga → Hamilton → Halifax → Ottawa
"""

import os
import re
import time
import json
import requests
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────────────────────
APIFY_TOKEN     = os.environ["APIFY_TOKEN"]
GHL_API_KEY     = os.environ["GHL_API_KEY"]
GHL_LOCATION_ID = os.environ["GHL_LOCATION_ID"]
SHEET_ID        = os.environ["GOOGLE_SHEET_ID"]
SA_JSON         = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

ACTOR_ID   = "easy_scraper~instagram-leads-scraper"
APIFY_BASE = "https://api.apify.com/v2"
GHL_URL    = "https://services.leadconnectorhq.com/contacts/"
GHL_TAGS   = ["cold-leads", "cold-email", "apify", "IG cold leads"]

TARGET_PER_CITY = 500

SHEET_HEADERS = ["Name", "Email", "Instagram URL", "Phone", "Status", "Date Added", "City"]

# ── City rotation ─────────────────────────────────────────────────────────────
CITIES = [
    "Toronto", "Vancouver", "Calgary", "Edmonton",
    "Mississauga", "Hamilton", "Halifax", "Ottawa",
]

CITY_KEYWORDS = {
    "Toronto": [
        "Toronto realtor",
        "Toronto real estate agent",
        "Toronto real estate broker",
        "Toronto homes agent",
        "realtor Toronto Ontario",
    ],
    "Vancouver": [
        "Vancouver realtor",
        "Vancouver real estate agent",
        "Vancouver real estate broker",
        "Vancouver homes agent",
        "realtor Vancouver BC",
    ],
    "Calgary": [
        "Calgary realtor",
        "Calgary real estate agent",
        "Calgary real estate broker",
        "Calgary homes agent",
        "realtor Calgary Alberta",
    ],
    "Edmonton": [
        "Edmonton realtor",
        "Edmonton real estate agent",
        "Edmonton real estate broker",
        "Edmonton homes agent",
        "realtor Edmonton Alberta",
    ],
    "Mississauga": [
        "Mississauga realtor",
        "Mississauga real estate agent",
        "Mississauga real estate broker",
        "Mississauga homes agent",
        "realtor Mississauga Ontario",
    ],
    "Hamilton": [
        "Hamilton realtor",
        "Hamilton real estate agent",
        "Hamilton real estate broker",
        "Hamilton Ontario homes agent",
        "realtor Hamilton Ontario",
    ],
    "Halifax": [
        "Halifax realtor",
        "Halifax real estate agent",
        "Halifax real estate broker",
        "Halifax Nova Scotia homes agent",
        "realtor Halifax Nova Scotia",
    ],
    "Ottawa": [
        "Ottawa realtor",
        "Ottawa real estate agent",
        "Ottawa real estate broker",
        "Ottawa homes agent",
        "realtor Ottawa Ontario",
    ],
}


def get_current_city(ws) -> tuple:
    """
    Read the City column (G) from the sheet, count leads per city,
    and return (city_to_scrape_today, {city: count}).
    """
    city_col = ws.col_values(7)   # column G
    counts = {city: 0 for city in CITIES}
    for val in city_col[1:]:      # skip header row
        val = val.strip()
        if val in counts:
            counts[val] += 1

    for city in CITIES:
        if counts[city] < TARGET_PER_CITY:
            return city, counts

    # All 8 cities complete — restart from Toronto
    print("[City] All cities have reached 500 leads! Restarting from Toronto.")
    return CITIES[0], counts


# ── Google Sheets ─────────────────────────────────────────────────────────────
def get_worksheet():
    creds = Credentials.from_service_account_info(
        SA_JSON,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    ws = gc.open_by_key(SHEET_ID).sheet1

    existing_headers = ws.row_values(1)

    if not existing_headers:
        # Fresh sheet — write full headers
        ws.append_row(SHEET_HEADERS, value_input_option="USER_ENTERED")
        ws.format("A1:G1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
        })
    elif "City" not in existing_headers:
        # Sheet exists but predates city column — add it
        ws.update_cell(1, 7, "City")
        ws.format("G1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
        })

    return ws


def get_existing_emails(ws) -> set:
    """Return lowercased set of all emails already in column B."""
    emails = ws.col_values(2)
    return {e.strip().lower() for e in emails[1:] if e.strip()}


def append_lead(ws, row_data: list) -> int:
    """Append a row and return its 1-indexed row number."""
    ws.append_row(row_data, value_input_option="USER_ENTERED")
    return len(ws.col_values(1))


def format_status(ws, row: int, success: bool):
    """Colour-code an already-written Status cell (column E). Uses batchUpdate quota."""
    cell = f"E{row}"
    if success:
        ws.format(cell, {
            "backgroundColor": {"red": 0.2, "green": 0.78, "blue": 0.35},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
        })
    else:
        ws.format(cell, {
            "backgroundColor": {"red": 0.9, "green": 0.2, "blue": 0.2},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
            },
        })


# ── Apify ─────────────────────────────────────────────────────────────────────
def run_apify(actor_input: dict) -> list:
    resp = requests.post(
        f"{APIFY_BASE}/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}",
        json=actor_input,
        timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    print(f"[Apify] Run started → {run_id}")

    terminal = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}
    while True:
        r = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}",
            timeout=15,
        )
        r.raise_for_status()
        data   = r.json()["data"]
        status = data["status"]
        print(f"[Apify] Status: {status}")
        if status in terminal:
            if status != "SUCCEEDED":
                raise RuntimeError(f"Apify run ended: {status}")
            dataset_id = data["defaultDatasetId"]
            break
        time.sleep(15)

    r = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items"
        f"?token={APIFY_TOKEN}&format=json&clean=true",
        timeout=60,
    )
    r.raise_for_status()
    items = r.json()
    print(f"[Apify] Fetched {len(items)} items")
    return items


# ── Name cleaning ─────────────────────────────────────────────────────────────
MATH_RANGES = [
    (0x1D400, 0x1D419, 'A'), (0x1D41A, 0x1D433, 'a'),  # bold
    (0x1D434, 0x1D44D, 'A'), (0x1D44E, 0x1D467, 'a'),  # italic
    (0x1D468, 0x1D481, 'A'), (0x1D482, 0x1D49B, 'a'),  # bold italic
    (0x1D5D4, 0x1D5ED, 'A'), (0x1D5EE, 0x1D607, 'a'),  # sans bold
    (0x1D608, 0x1D621, 'A'), (0x1D622, 0x1D63B, 'a'),  # sans italic
    (0x1D63C, 0x1D655, 'A'), (0x1D656, 0x1D66F, 'a'),  # sans bold italic
    (0x1D670, 0x1D689, 'A'), (0x1D68A, 0x1D6A3, 'a'),  # monospace
    (0x1D4D0, 0x1D4E9, 'A'), (0x1D4EA, 0x1D503, 'a'),  # bold script
    (0x1D5A0, 0x1D5B9, 'A'), (0x1D5BA, 0x1D5D3, 'a'),  # sans (non-bold)
]
SMALL_CAP_MAP = {
    0x1D00: 'A', 0x0299: 'B', 0x1D04: 'C', 0x1D05: 'D', 0x1D07: 'E',
    0x0262: 'G', 0x029C: 'H', 0x026A: 'I', 0x1D0A: 'J', 0x1D0B: 'K',
    0x029F: 'L', 0x1D0D: 'M', 0x0274: 'N', 0x1D0F: 'O', 0x1D18: 'P',
    0x0280: 'R', 0xA731: 'S', 0x1D1B: 'T', 0x1D1C: 'U', 0x1D20: 'V',
    0x1D21: 'W', 0x028F: 'Y', 0x1D22: 'Z',
}
NON_NAME_KEYWORDS = [
    "real estate", "realtor", "realty", "broker", "agent", "property",
    "homes", "housing", "mortgage", "sold by", "remax", "re/max",
    "keller", "century 21", "exp realty", "royal lepage", "inc", "llc",
    "ltd", "corp", "group", "team", "toronto", "vancouver", "calgary",
    "edmonton", "montreal", "ottawa", "niagara", "gta", "yyc", "yeg",
    "london", "miami", "chicago", "austin", "san diego", "content",
]


def _unicode_to_ascii(text: str) -> str:
    result = []
    for ch in text:
        cp = ord(ch)
        converted = False
        for start, end, base in MATH_RANGES:
            if start <= cp <= end:
                result.append(chr(ord(base) + (cp - start)))
                converted = True
                break
        if not converted:
            result.append(SMALL_CAP_MAP.get(cp, ch))
    return ''.join(result)


def _strip_emoji(text: str) -> str:
    text = re.sub(r"[\U00010000-\U0010ffff]", "", text)
    text = re.sub(r"[®™©♦◼▪️🔑🏡🏠📧📬✉️💌💼👤]", "", text)
    return text.strip()


def _is_person_name(name: str) -> bool:
    if not name or not name.strip():
        return False
    n = _strip_emoji(name).lower()
    if any(kw in n for kw in NON_NAME_KEYWORDS):
        return False
    words = name.split()
    if len(words) == 1 and len(name) > 15:
        return False
    return True


def _clean_person_name(raw: str) -> str:
    if not raw or not raw.strip():
        return ""
    name = _unicode_to_ascii(raw)
    name = _strip_emoji(name)
    name = re.split(r"\s*[|/–•]\s*", name)[0].strip()
    name = re.sub(
        r",\s*(Broker|Realtor|Realty|Agent|Real Estate|PREC|REALTOR|"
        r"PMP|B\.Sc|MBA|Courtier|Brokerage).*$",
        "", name, flags=re.IGNORECASE,
    ).strip()
    name = re.sub(r"\s*[\(\[].*?[\)\]].*$", "", name).strip()
    name = re.sub(r"[®™©]", "", name).strip()
    for kw in ["Real Estate", "Realtor", "Realty", "Agent", "Broker",
               "REALTOR", "GTA", "YYC", "YEG", "Content"]:
        name = re.sub(rf"\b{kw}\b", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"\s{2,}", " ", name).strip()
    name = name.strip("-– ")
    if name.isupper() and len(name) > 3:
        name = name.title()
    return name


def _extract_from_bio(bio: str) -> str:
    if not bio:
        return ""
    lines = [_strip_emoji(l).strip() for l in bio.split("\n") if l.strip()]
    for line in lines[:4]:
        line_clean = re.sub(r"[^\w\s\.\-']", " ", line).strip()
        line_clean = re.sub(r"\s+", " ", line_clean).strip()
        if not line_clean:
            continue
        if any(kw in line_clean.lower()
               for kw in NON_NAME_KEYWORDS + ["llc", "inc", "ltd", "prec", "remax"]):
            continue
        words = line_clean.split()
        if 2 <= len(words) <= 4 and all(
            w[0].isupper() for w in words if w and w[0].isalpha()
        ):
            return line_clean
    return ""


def resolve_name(full_name: str, bio: str) -> str:
    raw = _unicode_to_ascii((full_name or "").strip())
    if _is_person_name(raw):
        cleaned = _clean_person_name(raw)
        if cleaned:
            return cleaned
    cleaned = _clean_person_name(raw)
    if cleaned and _is_person_name(cleaned):
        return cleaned
    from_bio = _extract_from_bio(bio)
    if from_bio:
        return from_bio
    return cleaned or raw.strip()


# ── Phone extraction ──────────────────────────────────────────────────────────
def extract_phone(bio: str) -> str:
    if not bio:
        return ""
    match = re.search(
        r'\b(\+?1[-.\s]?)?\(?([2-9]\d{2})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})\b',
        bio,
    )
    if not match:
        return ""
    digits = re.sub(r'\D', '', match.group())
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return ""


# ── GoHighLevel ───────────────────────────────────────────────────────────────
def create_ghl_contact(name: str, email: str, instagram_url: str, phone: str) -> bool:
    parts = name.strip().split(None, 1)
    payload = {
        "firstName":  parts[0] if parts else name,
        "lastName":   parts[1] if len(parts) > 1 else "",
        "email":      email,
        "locationId": GHL_LOCATION_ID,
        "tags":       GHL_TAGS,
        "website":    instagram_url,
        "source":     "Instagram Scraper",
    }
    if phone:
        payload["phone"] = phone

    headers = {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type":  "application/json",
        "Version":       "2021-07-28",
    }
    try:
        resp = requests.post(GHL_URL, json=payload, headers=headers, timeout=30)
        if resp.status_code in (200, 201):
            return True
        print(f"  [GHL] {resp.status_code} for {email}: {resp.text[:300]}")
        return False
    except Exception as exc:
        print(f"  [GHL] Exception for {email}: {exc}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"  IG Lead Scraper — {today} UTC")
    print(f"{'='*60}\n")

    # 1. Load sheet and determine which city to scrape today
    ws = get_worksheet()
    existing = get_existing_emails(ws)
    current_city, city_counts = get_current_city(ws)

    print(f"[City] Today's target: {current_city}")
    print(f"[City] Progress:")
    for city in CITIES:
        bar = "█" * (city_counts[city] // 25)  # 1 block per 25 leads
        print(f"        {city:<12} {city_counts[city]:>3}/500  {bar}")
    print()

    # 2. Scrape via Apify using city-specific keywords
    actor_input = {
        "keywords":          CITY_KEYWORDS[current_city],
        "country":           "Canada",
        "collectEmails":     True,
        "maxLeadsPerKeyword": 50,
    }
    items = run_apify(actor_input)

    # 3. Filter to email-only + deduplicate within this batch
    seen: dict = {}
    for item in items:
        email = (item.get("email") or "").strip()
        if not email:
            continue
        key = email.lower()
        if key not in seen:
            seen[key] = item
    print(f"[Filter] {len(seen)} unique leads with email")

    # 4. Skip leads already in the sheet
    new_leads = {k: v for k, v in seen.items() if k not in existing}
    print(f"[Sheet]  {len(existing)} existing | {len(new_leads)} new\n")

    if not new_leads:
        print("[Done] No new leads today — nothing to add.")
        return

    # 5. Push to GHL first (collect results in memory — no sheet writes yet)
    sent = failed = skipped = 0
    batch = []   # {name, email, ig_url, phone, success}

    for email, item in new_leads.items():
        full_name = (item.get("full_name") or "").strip()
        bio       = item.get("bio") or ""
        ig_url    = item.get("profile_url") or ""

        name  = resolve_name(full_name, bio)
        phone = extract_phone(bio)

        if not name:
            print(f"[Skip]  No name resolved for {email}")
            skipped += 1
            continue

        success = create_ghl_contact(name, email, ig_url, phone)
        batch.append({"name": name, "email": email, "ig_url": ig_url,
                      "phone": phone, "success": success})

        if success:
            sent += 1
            print(f"[OK]    {name} <{email}>  [{current_city}]")
        else:
            failed += 1
            print(f"[FAIL]  {name} <{email}>  [{current_city}]")

        time.sleep(0.4)

    if not batch:
        print("[Done] All leads skipped — nothing to write.")
        return

    # 6. Batch write to Google Sheets (minimises API quota usage)
    print(f"\n[Sheet] Writing {len(batch)} rows in batch...")

    # One append call for all rows (1 values.append request)
    rows = [[b["name"], b["email"], b["ig_url"], b["phone"], "", today, current_city]
            for b in batch]
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    time.sleep(3)

    # One update call for all status text (1 values.update request)
    first_row = len(ws.col_values(1)) - len(batch) + 1
    status_values = [["Sent to GHL"] if b["success"] else ["Failed"] for b in batch]
    ws.update(values=status_values, range_name=f"E{first_row}:E{first_row + len(batch) - 1}")
    time.sleep(3)

    # Format status cells (uses spreadsheets.batchUpdate — separate, more lenient quota)
    print(f"[Sheet] Formatting {len(batch)} status cells...")
    for i, b in enumerate(batch):
        format_status(ws, first_row + i, b["success"])
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"  City: {current_city}")
    print(f"  Sent: {sent}  |  Failed: {failed}  |  Skipped: {skipped}")
    updated_count = city_counts[current_city] + sent
    print(f"  {current_city} total: {updated_count}/500")
    if updated_count >= TARGET_PER_CITY:
        next_city = CITIES[(CITIES.index(current_city) + 1) % len(CITIES)]
        print(f"  ✓ {current_city} complete! Tomorrow: {next_city}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
