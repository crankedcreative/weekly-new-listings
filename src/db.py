import sqlite3
from pathlib import Path
from typing import Dict, Any, Iterable

DB_PATH = Path("data/listings.sqlite3")

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange TEXT NOT NULL,
  ticker TEXT,
  company TEXT,
  listing_date TEXT,
  address TEXT,
  website TEXT,
  shares_outstanding TEXT,
  source_url TEXT,
  discovered_utc TEXT NOT NULL,
  UNIQUE(exchange, ticker, source_url)
);
"""

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH.as_posix())
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(SCHEMA)
    return conn

def upsert_many(rows: Iterable[Dict[str, Any]]) -> int:
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO listings
                (exchange, ticker, company, listing_date, address, website, shares_outstanding, source_url, discovered_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r.get("exchange"),
                    r.get("ticker"),
                    r.get("company"),
                    r.get("listing_date"),
                    r.get("address"),
                    r.get("website"),
                    r.get("shares_outstanding"),
                    r.get("source_url"),
                    r.get("discovered_utc"),
                ),
            )
            if cur.rowcount == 1:
                inserted += 1
        except Exception:
            # keep going on one bad row
            pass

    conn.commit()
    conn.close()
    return inserted

def fetch_weekly_new(since_utc_iso: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT exchange, ticker, company, listing_date, address, website, shares_outstanding, source_url, discovered_utc
        FROM listings
        WHERE discovered_utc >= ?
        ORDER BY discovered_utc DESC
        """,
        (since_utc_iso,),
    )
    rows = cur.fetchall()
    conn.close()
    cols = ["exchange","ticker","company","listing_date","address","website","shares_outstanding","source_url","discovered_utc"]
    return [dict(zip(cols, r)) for r in rows]
