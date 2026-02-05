import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

HEADERS = {"User-Agent": "Mozilla/5.0"}
CSE_BULLETINS_HUB = "https://thecse.com/news-events/bulletins/"

def _get(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.text

def _text(soup):
    return soup.get_text("\n", strip=True)

def fetch_cse_new_listings(seed_limit=200):
    html = _get(CSE_BULLETINS_HUB)
    soup = BeautifulSoup(html, "html.parser")

    # Collect issuer listing URLs found in the hub page
    issuer_urls = []
    for a in soup.select("a[href*='/listings/']"):
        href = a.get("href")
        if not href:
            continue
        url = requests.compat.urljoin(CSE_BULLETINS_HUB, href)
        issuer_urls.append(url)

    issuer_urls = list(dict.fromkeys(issuer_urls))[:seed_limit]

    out = []
    for issuer in issuer_urls:
        bulletins_url = issuer.rstrip("/") + "/bulletins/"
        try:
            bhtml = _get(bulletins_url)
        except Exception:
            continue

        bsoup = BeautifulSoup(bhtml, "html.parser")
        t = _text(bsoup)

        # If the issuer bulletins list contains "New Listing", capture it
        if "New Listing" not in t:
            continue

        # Try to extract ticker from the page header pattern
        # Many pages show e.g. "Eagle Royalties Ltd." + symbol near top. :contentReference[oaicite:7]{index=7}
        ticker = None
        m = re.search(r"\n([A-Z]{1,5})\s*\n#\s", t)
        if m:
            ticker = m.group(1).strip()

        company = None
        # crude: title or first H1
        h1 = bsoup.find(["h1","h2"])
        if h1:
            company = h1.get_text(strip=True)

        # Issued & Outstanding appears on many issuer pages (not always on bulletins page).
        shares_outstanding = None
        m2 = re.search(r"Issued\s*&\s*Outstanding\s+([0-9,]+)", t)
        if m2:
            shares_outstanding = m2.group(1)

        out.append({
            "exchange": "CSE",
            "ticker": ticker,
            "company": company,
            "shares_outstanding": shares_outstanding,
            "source_url": bulletins_url,
            "discovered_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        time.sleep(0.2)

    return out
