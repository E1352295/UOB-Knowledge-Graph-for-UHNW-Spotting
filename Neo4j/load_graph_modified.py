
#!/usr/bin/env python3
"""load_graph.py – Ingest three UHNW‑related datasets into Neo4j Aura.

* SGX annual‑report export (Cypher JSON)     → ingest_neo4j_query_table()
* Simplified Wikidata JSON                   → ingest_wikidata_json()
* MAS personnel list (CSV)                  → ingest_mas_csv()

This version automatically flattens non‑primitive properties, handles missing
files gracefully, and supports the new MAS CSV layout exported from the
Monetary Authority of Singapore Financial Institutions Directory.

CSV expectations
────────────────
The MAS_Personnel_merged.csv file bundled in this repo contains **at least**
these case‑insensitive columns

    Company Name, Person Name, Person Title

Additional columns such as “href[0]” are ignored.  Only rows that have both
*Person Name* and *Company Name* are ingested.

Environment
────────────
Set the three variables below to override Neo4j connection settings; otherwise
defaults are fine for local testing:

    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
"""

import os, json, csv, argparse
from pathlib import Path
from typing import Dict, Any, Optional

from neo4j import GraphDatabase

# ──────────────────────────────────────────────────────────────────────────────
# Config – environment variables fall back to sensible defaults for dev use
# ──────────────────────────────────────────────────────────────────────────────

NEO4J_URI      = os.getenv("NEO4J_URI",      "neo4j+s://d8d4e86b.databases.neo4j.io")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "IVVi_p1Rl2ca-O5g5ULkd5KHtg2uSXkLaj1So_oHL4Q")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ──────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ──────────────────────────────────────────────────────────────────────────────

def flatten_props(props: Dict[str, Any]) -> Dict[str, Any]:
    """Return only values that Neo4j accepts as property values."""
    return {k: v for k, v in props.items() if isinstance(v, (str, int, float, bool, list))}

# ──────────────────────────────────────────────────────────────────────────────
# Merge helpers (Person, Company, Role, Family)
# ──────────────────────────────────────────────────────────────────────────────

def merge_person(tx, id: str, name: str, props: Dict[str, Any]):
    safe_props = flatten_props(props)
    tx.run(
        """
        MERGE (p:Person {id: $id})
        ON CREATE SET p.name = $name, p += $safe_props
        """,
        id=id,
        name=name,
        safe_props=safe_props,
    )


def merge_company(tx, id: str, name: str, props: Dict[str, Any]):
    safe_props = flatten_props(props)
    tx.run(
        """
        MERGE (c:Company {id: $id})
        ON CREATE SET c.name = $name, c += $safe_props
        """,
        id=id,
        name=name,
        safe_props=safe_props,
    )


def merge_role(
    tx,
    person_id: str,
    company_id: str,
    role: str,
    start: Optional[int],
    end: Optional[int],
    source: str,
):
    tx.run(
        """
        MATCH (p:Person {id: $person_id})
        MATCH (c:Company {id: $company_id})
        MERGE (p)-[r:HAS_ROLE {role: $role, source: $source}]->(c)
        ON CREATE SET r.start = $start, r.end = $end
        """,
        person_id=person_id,
        company_id=company_id,
        role=role,
        start=start,
        end=end,
        source=source,
    )


def merge_family(tx, src_id: str, dst_id: str, relation: str, source: str):
    tx.run(
        """
        MATCH (src:Person {id: $src_id})
        MATCH (dst:Person {id: $dst_id})
        MERGE (src)-[:HAS_FAMILY {relation: $relation, source: $source}]->(dst)
        """,
        src_id=src_id,
        dst_id=dst_id,
        relation=relation,
        source=source,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Ingest – SGX annual‑report JSON (export from Neo4j Browser)
# ──────────────────────────────────────────────────────────────────────────────

def ingest_neo4j_query_table(path: Path):
    data = json.loads(path.read_text(encoding="utf‑8"))
    with driver.session() as session:
        for rec in data:
            p_node = rec["n"]
            c_node = rec.get("c")
            r_rel  = rec.get("r")

            if not p_node:
                continue

            pid = f"src1:{p_node['identity']}"
            session.execute_write(
                merge_person, pid, p_node["properties"].get("name", ""), p_node["properties"]
            )

            if c_node and r_rel:
                cid = f"src1:{c_node['identity']}"
                session.execute_write(
                    merge_company, cid, c_node["properties"].get("name", ""), c_node["properties"]
                )
                start = r_rel["properties"].get("start_date")
                end   = r_rel["properties"].get("end_date")
                session.execute_write(
                    merge_role,
                    pid,
                    cid,
                    r_rel["type"],
                    start,
                    end,
                    "neo4j_export",
                )

# ──────────────────────────────────────────────────────────────────────────────
# Ingest – Simplified Wikidata JSON
# ──────────────────────────────────────────────────────────────────────────────

def ingest_wikidata_json(path: Path):
    persons = json.loads(path.read_text(encoding="utf‑8"))["persons"]
    with driver.session() as session:
        # Pass‑1: nodes
        for p in persons:
            wid = f"wd:{p['id']}"
            session.execute_write(merge_person, wid, p.get("props", {}).get("name", wid), p.get("props", {}))

        # Pass‑2: relationships (family)
        for p in persons:
            src_id = f"wd:{p['id']}"
            for tag, label in p.get("family", {}).items():
                if not tag.endswith("Label"):
                    continue
                relation = tag.replace("Label", "").lower()
                targets = label if isinstance(label, list) else [label]
                for name in targets:
                    dst_id = f"wd:label:{name}"
                    session.execute_write(merge_person, dst_id, name, {})
                    session.execute_write(merge_family, src_id, dst_id, relation, "wikidata_json")

# ──────────────────────────────────────────────────────────────────────────────
# Ingest – MAS personnel list (CSV)
# ──────────────────────────────────────────────────────────────────────────────

def ingest_mas_csv(path: Path):
    """Parse the MAS_Personnel_merged.csv exported from MAS FID.

    We look only for three *case‑insensitive* headers:

        Company Name, Person Name, Person Title

    Rows missing *Person Name* **or** *Company Name* are silently skipped.
    """
    if not path.exists():
        print(f"[ingest_mas_csv] WARNING: {path} not found; skipping MAS import.")
        return

    with path.open(newline="", encoding="utf‑8‑sig") as handle:
        reader = csv.DictReader(handle)
        with driver.session() as session:
            for row in reader:
                # normalise keys
                row_norm = {k.lower().strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                person_name  = row_norm.get("person name")
                company_name = row_norm.get("company name")
                role         = row_norm.get("person title", "")
                if not (person_name and company_name):
                    continue

                pid = f"mas:{person_name.lower().replace(' ', '_')}"
                cid = f"mas:{company_name.lower().replace(' ', '_')}"

                session.execute_write(merge_person,  pid, person_name,  {})
                session.execute_write(merge_company, cid, company_name, {})
                session.execute_write(merge_role, pid, cid, role, None, None, "MAS_csv")

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────

def main(args):
    # Uncomment the lines you need – they are left off by default for faster iteration.
    # ingest_neo4j_query_table(Path(args.neo4j_export))
    ingest_wikidata_json(Path(args.wikidata_json))
    ingest_mas_csv(Path(args.mas_personnel_csv))
    driver.close()


if __name__ == "__main__":
    argp = argparse.ArgumentParser(description="Load UHNW datasets into Neo4j Aura")
    argp.add_argument(
        "--neo4j_export",
        default="G:/My Drive/NUS MSBA SEM2/UOB/SGX Annual Reports/Case Study/Venture Corporation Limited/neo4j_query_table_data_2025-6-26.json",
        help="SGX annual‑report export (Cypher JSON)",
    )
    argp.add_argument(
        "--wikidata_json",
        default="G:/My Drive/NUS MSBA SEM2/UOB/WikiData/data.json",
        help="Simplified Wikidata JSON",
    )
    argp.add_argument(
        "--mas_personnel_csv",
        default="G:/My Drive/NUS MSBA SEM2/UOB/MAS/MAS_Personnel_merged.csv",
        help="MAS personnel list (CSV, exported from MAS FID)",
    )
    main(argp.parse_args())
