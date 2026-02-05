import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path

from sources_tsxv import fetch_tsxv_new_listings
from sources_cse import fetch_cse_new_listings
from db import upsert_many, fetch_weekly_new

OUT_DIR = Path("output")

def write_csv(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def write_summary(rows, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tsxv = [r for r in rows if r.get("exchange") == "TSX-V"]
    cse  = [r for r in rows if r.get("exchange") == "CSE"]

    lines = []
    lines.append(f"# Weekly New Listings Summary")
    lines.append(f"- Generated (UTC): {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    lines.append(f"- Total new records this week: **{len(rows)}**")
    lines.append(f"- TSX-V: **{len(tsxv)}** | CSE: **{len(cse)}**")
    lines.append("")
    lines.append("## Quick List")
    for r in rows[:50]:
        lines.append(f"- **{r.get('exchange')}** {r.get('ticker') or ''} — {r.get('company') or ''}  \n  {r.get('source_url')}")
    path.write_text("\n".join(lines), encoding="utf-8")

def main():
    # 1) collect
    tsxv_rows = fetch_tsxv_new_listings()
    cse_rows  = fetch_cse_new_listings()

    all_rows = tsxv_rows + cse_rows

    # 2) store / dedupe
    inserted = upsert_many(all_rows)

    # 3) weekly window: last 7 days
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")
    weekly = fetch_weekly_new(since)

    # 4) output
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(weekly, OUT_DIR / "new_listings_latest.csv")
    write_summary(weekly, OUT_DIR / "new_listings_summary.md")

    print(f"Collected: {len(all_rows)} rows | Inserted new: {inserted} | Weekly rows: {len(weekly)}")

if __name__ == "__main__":
    main()
