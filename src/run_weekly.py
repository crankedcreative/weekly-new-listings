from __future__ import annotations

import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple

from sources_tsxv import fetch_tsxv_new_listings
from sources_cse import fetch_cse_new_listings
from db import upsert_many, fetch_weekly_new

OUT_DIR = Path("output")


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure we always create a CSV, even if empty
    if not rows:
        fieldnames = [
            "exchange",
            "ticker",
            "company",
            "listing_date",
            "address",
            "website",
            "shares_outstanding",
            "source_url",
            "discovered_utc",
        ]
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
        return

    fieldnames = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_summary(
    rows: List[Dict[str, Any]],
    path: Path,
    warnings: List[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    tsxv = [r for r in rows if r.get("exchange") == "TSX-V"]
    cse = [r for r in rows if r.get("exchange") == "CSE"]

    lines: List[str] = []
    lines.append("# Weekly New Listings Summary")
    lines.append(f"- Generated (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    lines.append(f"- Total rows (last 7 days): **{len(rows)}**")
    lines.append(f"- TSX-V rows: **{len(tsxv)}** | CSE rows: **{len(cse)}**")
    lines.append("")

    if warnings:
        li
