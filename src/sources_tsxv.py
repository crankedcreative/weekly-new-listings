"""
TSX Venture "New Listing" collector (hardened for GitHub Actions).

Strategy:
1) Pull "LatestCompanyDocuments" from infoventure host (more reliable than apps.tmx.com).
2) Extract links to NoticesContents bulletin pages.
3) Open each bulletin and keep only those with "BULLETIN TYPE: New Listing".
4) Parse ticker/company and bulletin date when possible.

Notes:
- Network timeouts happen. This file retries and backs off.
- One broken page should not fail the whole job.
"""

from __future__ import annotations

import random
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Prefer infoventure for both seed + bulletin pages
TMX_HOST = "https://infoventure.tsx.com/TSXVenture/TSXVentureHttpController"
SEED_PARAMS = {"BulletinsMode": "on", "GetPage": "LatestCompanyDocuments", "NewsReleases": "off"}

# If you ever want a fallback, you can add apps.tmx.com here, but it timed out for you:
# FALLBACK_HOST = "https://apps.tmx.com/TSXVenture/TSXVentureHttpController"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _get_text(
    s: requests.Session,
    url: str,
    params: Optional[dict] = None,
    max_attempts: int = 6,
) -> str:
    """
    Robust GET with exponential backoff + jitter.
    Timeout is (connect, read).
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = s.get(url, params=params, timeout=(20, 90))
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
            # backoff: 2,4,8,16,32... capped + jitter
            sleep_s = min(60.0, (2.0 ** attempt)) + random.uniform(0.0, 1.8)
            time.sleep(sleep_s)

    # If we reach here, all attempts failed
    assert last_err is not None
    raise last_err


def _unique_keep_order(items: list[str]) -> list[str]:
    seen = set()
    out: list[str] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _parse_company_ticker(page_text: str) -> tuple[Optional[str], Optional[str]]:
    """
    Attempt to parse:
      COMPANY NAME ("TICK")
    from bulletin page text.
    """
    m = re.search(
        r"\n\s*BULLETIN\s+V\d{4}-\d+.*?\n\s*(.+?)\s+\(\"?([A-Z0-9\.\-]+)\"?\)",
        page_text,
        re.S,
    )
    if not m:
        return None, None
    company = m.group(1).strip()
    ticker = m.group(2).strip()
    return company, ticker


def _parse_bulletin_date(page_text: str) -> Optional[str]:
    m = re.search(r"BULLETIN DATE:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", page_text)
    return m.group(1).strip() if m else None


def fetch_tsxv_new_listings(seed_limit: int = 300) -> list[dict]:
    """
    Returns list of dicts with:
      exchange, ticker, company, listing_date, source_url, discovered_utc
    """
    s = _session()

    # Small initial delay reduces rate-limit spikes on some hosts
    time.sleep(0.8)

    # 1) Seed page
    html = _get_text(s, TMX_HOST, params=SEED_PARAMS)
    soup = BeautifulSoup(html, "html.parser")

    # 2) Extract bulletin content links
    links: list[str] = []
    for a in soup.select("a[href*='GetPage=NoticesContents']"):
        href = a.get("href")
        if not href:
            continue
        full = requests.compat.urljoin(TMX_HOST, href)
        # Normalize to infoventure host
        full = full.replace("apps.tmx.com/TSXVenture/TSXVentureHttpController", TMX_HOST)
        full = full.replace("infoventure.tsx.com/TSXVenture/TSXVentureHttpController", TMX_HOST)
        links.append(full)

    links = _unique_keep_order(links)[:seed_limit]

    out: list[dict] = []
    for i, link in enumerate(links, start=1):
        try:
            page = _get_text(s, link)
        except Exception as e:
            print(f"[WARN] TSX-V bulletin fetch failed ({i}/{len(links)}): {link} :: {e}")
            continue

        # Filter to "New Listing" bulletin type
        if "BULLETIN TYPE" not in page or "BULLETIN TYPE: New Listing" not in page:
            continue

        company, ticker = _parse_company_ticker(page)
        listing_date = _parse_bulletin_date(page)

        out.append(
            {
                "exchange": "TSX-V",
                "ticker": ticker,
                "company": company,
                "listing_date": listing_date,
                "source_url": link,
                "discovered_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )

        # polite pacing
        time.sleep(0.25)

    return out
