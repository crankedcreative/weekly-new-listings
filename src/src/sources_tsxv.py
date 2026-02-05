import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

HEADERS = {"User-Agent": "Mozilla/5.0"}

TMX_LATEST_URL = "https://apps.tmx.com/TSXVenture/TSXVentureHttpController"
TMX_BULLETIN_HOST = "https://infoventure.tsx.com/TSXVenture/TSXVentureHttpController"

def _get(url, params=None):
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def fetch_tsxv_new_listings(seed_limit=250):
    # Seed page (short window). We run weekly, but this still picks up recent bulletins.
    html = _get(TMX_LATEST_URL, params={"BulletinsMode":"on","GetPage":"LatestCompanyDocuments","NewsReleases":"off"})
    soup = BeautifulSoup(html, "html.parser")

    # Collect bulletin content links
    links = []
    for a in soup.select("a[href*='GetPage=NoticesContents']"):
        href = a.get("href")
        if not href:
            continue
        if "infoventure.tsx.com" in href:
            links.append(href)
        else:
            links.append(requests.compat.urljoin(TMX_LATEST_URL, href))

    links = list(dict.fromkeys(links))[:seed_limit]  # unique, capped

    out = []
    for link in links:
        # Prefer infoventure host if we can
        link2 = link.replace("apps.tmx.com/TSXVenture/TSXVentureHttpController", TMX_BULLETIN_HOST)
        try:
            page = _get(link2)
        except Exception:
            continue

        if "BULLETIN TYPE" not in page:
            continue

        # Filter to New Listing variants
        if "BULLETIN TYPE: New Listing" not in page:
            continue

        # Try to parse company + ticker from the bulletin header line:
        # Example format: PANTERA SILVER CORP. ("PNTR") or similar. :contentReference[oaicite:5]{index=5}
        m = re.search(r"\n\s*BULLETIN\s+V\d{4}-\d+.*?\n\s*(.+?)\s+\(\"?([A-Z0-9\.\-]+)\"?\)", page, re.S)
        company = m.group(1).strip() if m else None
        ticker = m.group(2).strip() if m else None

        # Bulletin Date
        mdate = re.search(r"BULLETIN DATE:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page)
        listing_date = mdate.group(1).strip() if mdate else None

        out.append({
            "exchange": "TSX-V",
            "ticker": ticker,
            "company": company,
            "listing_date": listing_date,
            "source_url": link2,
            "discovered_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        time.sleep(0.2)

    return out
