from __future__ import annotations

import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

from sources_tsxv import fetch_tsxv_new_listings
from sources_cse import fetch_cse_new_listings
from db import upsert_many, fetch_weekly_new

OUT_DIR = Path("output")


def write_csv(rows: List[Dict[str, Any]], path: Path) -> None:
    """
    Always writes a CSV file. If rows is empty, writes header only.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    default_fields = [
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

    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=default_fields)
            w.writeheader()
        return

    fieldnames = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_summary(rows: List[Dict[str, Any]], path: Path, warnings: List[str]) -> None:
    """
    Always writes a markdown summary file.
    """
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
        lines.append("## Warnings")
        for w in warnings:
            lines.append(f"- {w}")
        lines.append("")

    lines.append("## Quick List (top 50)")
    if not rows:
        lines.append("- No rows returned this week (or sources were unreachable).")
    else:
        for r in rows[:50]:
            lines.append(
                f"- **{r.get('exchange')}** {r.get('ticker') or ''} — {r.get('company') or ''}\n"
                f"  - Source: {r.get('source_url')}\n"
            )

    path.write_text("\n".join(lines), encoding="utf-8")


def safe_collect(fn, label: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Run a collector safely. Returns (rows, warning_or_None).
    """
    try:
        rows = fn()
        return rows, None
    except Exception as e:
        return [], f"{label} fetch failed (continuing): {type(e).__name__}: {e}"


def main() -> None:
    warnings: List[str] = []

    # Ensure output dir + placeholder files always exist (so artifacts upload)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "new_listings_latest.csv").touch(exist_ok=True)
    (OUT_DIR / "new_listings_summary.md").touch(exist_ok=True)

    # 1) Collect (soft-fail each source)
    tsxv_rows, w1 = safe_collect(fetch_tsxv_new_listings, "TSX-V")
    if w1:
        print(f"[WARN] {w1}")
        warnings.append(w1)

    cse_rows, w2 = safe_collect(fetch_cse_new_listings, "CSE")
    if w2:
        print(f"[WARN] {w2}")
        warnings.append(w2)

    all_rows = tsxv_rows + cse_rows

    # 2) Store / de-dupe (soft-fail; still output files)
    inserted = 0
    try:
        inserted = upsert_many(all_rows)
    except Exception as e:
        w = f"DB upsert failed (continuing): {type(e).__name__}: {e}"
        print(f"[WARN] {w}")
        warnings.append(w)

    # 3) Weekly window from DB (last 7 days). If DB read fails, fall back to what we collected now.
    weekly: List[Dict[str, Any]] = []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
        weekly = fetch_weekly_new(since)
    except Exception as e:
        w = f"DB fetch_weekly_new failed (continuing): {type(e).__name__}: {e}"
        print(f"[WARN] {w}")
        warnings.append(w)
        weekly = all_rows

    # 4) Write outputs (always)
    write_csv(weekly, OUT_DIR / "new_listings_latest.csv")
    write_summary(weekly, OUT_DIR / "new_listings_summary.md", warnings)

    print(
        f"Collected: {len(all_rows)} rows | Inserted new: {inserted} | "
        f"Weekly rows (output): {len(weekly)}"
    )
    # No unhandled exceptions => GitHub Actions step returns success


if __name__ == "__main__":
    main()
