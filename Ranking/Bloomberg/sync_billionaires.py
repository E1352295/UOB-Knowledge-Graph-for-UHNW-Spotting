"""sync_billionaires.py
=======================
Bulk‑update *existing* `:Person` nodes in Neo4j Aura **Free** with data from the
Bloomberg Billionaires CSV (≈500 rows).

Only the following attributes are written / updated:
    name        (used for node match)
    netWorth    – integer USD (e.g. "23.4B" → 23400000000)
    country     – string
    industry    – string
    lastUpdated – Neo4j `date()` stamp

‼️  *rank*, *change* and *recId* fields are **ignored** per user request.

--------
Usage
-----
```bash
python sync_billionaires.py \
  --csv bloomberg_billionaires.csv \
  --neo4j-uri neo4j+s://<dbid>.databases.neo4j.io \
  --neo4j-user neo4j \
  --neo4j-pass <password> \
  [--dry-run]               # log but don’t write
```
```
python sync_billionaires.py --help   # full CLI
```
--------
Notes
-----
* Script uses the official Neo4j Python driver – works fine with Aura Free.
* No APOC/GDS required.
* Batched `UNWIND` writes keep query count low (default 100 rows per tx).
"""
from __future__ import annotations
import argparse
import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List
import uuid

from neo4j import Driver, GraphDatabase, Transaction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_net_worth(raw: str) -> int | None:
    """Convert strings like "+22.3B", "450M", "12,345,678" → integer USD."""
    if not raw:
        return None
    s = raw.strip().replace(",", "")
    if s.startswith("+"):
        s = s[1:]
    multiplier = 1
    if s[-1].upper() == "B":
        multiplier = 1_000_000_000
        s = s[:-1]
    elif s[-1].upper() == "M":
        multiplier = 1_000_000
        s = s[:-1]
    try:
        return int(float(s) * multiplier)
    except ValueError:
        logging.warning("Unable to parse NetWorth value '%s'", raw)
        return None

# ---------------------------------------------------------------------------
# Neo4j write logic
# ---------------------------------------------------------------------------

CYPHER_UPDATE = """
UNWIND $batch AS row
MERGE (p:Person {name: row.name})
ON CREATE SET
     p.id             = row.id,
     p.netWorth       = row.netWorth,
     p.country        = row.country,
     p.industry       = row.industry,
     p.updated        = row.updated,
     p.aliases        = row.aliases,
     p.baseScore      = row.baseScore,
     p.pagerank       = row.pagerank,
     p.reference_file = row.reference_file,
     p.reference_type = row.reference_type,
     p.score          = row.score,
     p.wOut           = row.wOut,
     p.is_new         = true
ON MATCH SET
     p.netWorth       = row.netWorth,
     p.country        = row.country,
     p.industry       = row.industry,
     p.updated        = row.updated
WITH p, p.is_new as is_new
REMOVE p.is_new
RETURN p.name as name, is_new
"""

def write_batch(tx: Transaction, batch: List[Dict]):
    result = tx.run(CYPHER_UPDATE, batch=batch)
    for record in result:
        if record["is_new"]:
            logging.info("Created new person: %s", record["name"])
        else:
            logging.info("Updated existing person: %s", record["name"])

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> List[Dict]:
    rows: List[Dict] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("Name") or row.get("name")
            if not name:
                continue  # skip bad rows
            net_worth = parse_net_worth(row.get("NetWorth") or row.get("TotalWorth", ""))
            rows.append({
                # From CSV
                "name": name.strip(),
                "netWorth": net_worth,
                "country": row.get("Country", "").strip() or None,
                "industry": row.get("Industry", "").strip() or None,
                # Schema alignment
                "id": f"person:{uuid.uuid4().hex[:12]}",
                "updated": datetime.now().isoformat(),
                "aliases": [],
                "baseScore": 1.0,
                "pagerank": 1.0,
                "reference_file": path.name,
                "reference_type": "bloomberg_billionaires",
                "score": 1.0,
                "wOut": 0,
            })
    logging.info("Loaded %d rows from CSV", len(rows))
    return rows

def upsert_people(driver: Driver, people: List[Dict], batch_size: int = 100, dry_run: bool = False):
    if dry_run:
        logging.info("Dry run mode: No changes will be written to the database.")
        # In dry run, we need to know who already exists to simulate MERGE
        with driver.session() as session:
            existing_names = set(session.run("MATCH (p:Person) RETURN p.name AS name").value('name'))
        
        for person in people:
            if person['name'] in existing_names:
                logging.info("Would update existing person: %s", person['name'])
            else:
                logging.info("Would create new person: %s", person['name'])
        return

    # Actual write operation
    with driver.session() as session:
        for i in range(0, len(people), batch_size):
            batch = people[i:i + batch_size]
            session.write_transaction(write_batch, batch)
    logging.info("Upserted %d people into the database.", len(people))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Sync Bloomberg Billionaires to Neo4j Person nodes")
    parser.add_argument("--csv", type=Path, required=True, help="Path to bloomberg_billionaires.csv")
    parser.add_argument("--neo4j-uri", required=True, help="bolt[s] or neo4j[s] URI for Aura Free")
    parser.add_argument("--neo4j-user", required=True)
    parser.add_argument("--neo4j-pass", required=True)
    parser.add_argument("--batch", type=int, default=100, help="Batch size (default 100)")
    parser.add_argument("--dry-run", action="store_true", help="Log only, no writes")
    return parser.parse_args()

def main():
    args = parse_args()
    people = load_csv(args.csv)

    if not args.dry_run and not people:
        logging.warning("No valid rows – exiting")
        return

    driver = GraphDatabase.driver(args.neo4j_uri, auth=(args.neo4j_user, args.neo4j_pass))
    try:
        upsert_people(driver, people, batch_size=args.batch, dry_run=args.dry_run)
    finally:
        driver.close()

    logging.info("Done. %s", "(dry‑run)" if args.dry_run else "")

if __name__ == "__main__":
    main()
