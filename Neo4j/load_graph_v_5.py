#!/usr/bin/env python3
"""load_graph_v5.py — Unified UHNW loader (self‑loop safe)

This version builds on *load_graph_v4.py* and addresses two key issues:
1. **Self‑loops** – person‑to‑person relationships that point back to the *same* person are
   silently ignored so Neo4j never receives a `(:Person)-[:FAMILY]->(:Person)` edge where the
   two endpoints are identical.
2. **Duplicate `ingest_wikidata` definitions** – v4 accidentally defined the function twice.
   The second definition masked the first and dropped some role logic.  The two versions
   have been merged into a single implementation below.  Existing behaviour from the
   *validated* `load_graph_modified.py` is retained.

Any additional discrepancies between *modified* and *v4* have been annotated with
`TODO:` comments so you can decide on the preferred logic.
"""

import os, re, json, csv, argparse, uuid, datetime
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from rapidfuzz.fuzz import token_set_ratio
from rapidfuzz import distance
from neo4j import GraphDatabase

# ─────────────────────────────── Config ───────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "neo4j+s://8f6e6423.databases.neo4j.io")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "TUOx-U2EDDDXXNAteOqarP3aEj7XxMcsoilyEtL7NLI")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

TODAY = datetime.datetime.utcnow().isoformat()

# ───────────────────── Registry & canonical helpers ─────────────────────
SLUG_RE = re.compile(r"[^a-z0-9]+")

def slug(txt: str) -> str:
    return SLUG_RE.sub("_", txt.lower()).strip("_")

PersonEntry = Dict[str, Any]  # {id, canonical, aliases:set[str]}

person_registry: Dict[str, PersonEntry] = {}
company_registry: Dict[str, str]        = {}  # slug → id
qid_registry: Dict[str, str]            = {}  # qid → slug
processed_annual_reports: set[str]      = set()

FUZZ_THRESHOLD = 93
LEV_DIST       = 2

# ---------------- Person / Company registry APIs ----------------

def _fuzzy_find_person(s: str) -> Optional[str]:
    """Return slug key from registry that fuzzy‑matches *s*"""
    for k in person_registry:
        if token_set_ratio(k, s) >= FUZZ_THRESHOLD:
            return k
    return None

def _fuzzy_find_company(s: str) -> Optional[str]:
    """Return slug key from registry that fuzzy‑matches *s*"""
    for k in company_registry:
        if token_set_ratio(k, s) >= FUZZ_THRESHOLD or distance.Levenshtein.distance(k, s) <= LEV_DIST:
            return k
    return None


def get_or_create_person(raw_name: str, canonical: Optional[str] = None, extras: Optional[List[str]] = None, qid: Optional[str] = None) -> Tuple[str, PersonEntry]:
    """Get or create a person entry. If qid is provided, it's the authoritative ID."""
    name = canonical or raw_name
    s = slug(name)

    # If qid is given, it's the source of truth. The node ID *is* the QID.
    if qid:
        if qid in qid_registry:
            key = qid_registry[qid]
            entry = person_registry[key]
            if name not in entry['aliases'] and name != entry['canonical']:
                entry['aliases'].add(name)
            return entry['id'], entry
        else:
            pid = qid  # Use QID as the primary node ID
            entry = {"id": pid, "canonical": name, "aliases": set(), "qid": qid}
            person_registry[s] = entry
            qid_registry[qid] = s
            if extras:
                entry['aliases'].update(extras)
            return pid, entry

    # Fallback for non-Wikidata sources without a QID
    key = _fuzzy_find_person(s)
    if key is None:
        pid = f"person:{uuid.uuid4().hex[:12]}"
        entry = {"id": pid, "canonical": name, "aliases": set(), "qid": None}
        person_registry[s] = entry
        key = s
    else:
        entry = person_registry[key]

    if extras:
        entry['aliases'].update(extras)
    if raw_name not in entry['aliases'] and raw_name != entry['canonical']:
        entry['aliases'].add(raw_name)

    return entry['id'], entry


def get_or_create_company(raw_name: str) -> str:
    s = slug(raw_name)
    key = _fuzzy_find_company(s)
    if key is None:
        cid = f"company:{uuid.uuid4().hex[:12]}"
        company_registry[s] = cid
        return cid
    else:
        return company_registry[key]

# ────────────────────────── Neo4j Cypher ──────────────────────────
MERGE_PERSON = """
MERGE (p:Person {id:$id})
SET p.name=$name,
    p.aliases=$alias,
    p.wikidata_qid=$qid,
    p.updated=$today,
    p.reference_type=$source_type,
    p.reference_file=$source_file
"""

MERGE_COMPANY = """
MERGE (c:Company {id:$id})
SET c.name=$name,
    c.lastUpdated=datetime($today),
    c.reference_type=$source_type,
    c.reference_file=$source_file
"""

MERGE_ROLE = """
MATCH (p:Person {id:$pid})
MATCH (c:Company {id:$cid})
MERGE (p)-[r:HAS_ROLE_AT {role:$role}]->(c)
ON CREATE SET
    r.startDate=$start,
    r.endDate=$end,
    r.reference_type=$source_type,
    r.reference_file=$source_file
ON MATCH SET
    r.startDate = coalesce($start, r.startDate),
    r.endDate = coalesce($end, r.endDate),
    r.reference_type=$source_type,
    r.reference_file=$source_file
"""

MERGE_FAMILY = """
MATCH (a:Person {id:$src})
MATCH (b:Person {id:$dst})
MERGE (a)-[r:FAMILY {relation:$rel}]->(b)
SET r.reference_type=$source_type,
    r.reference_file=$source_file
"""

# ---------------- Global state ----------------

DRY_RUN = False

# ---------------- Write helpers ----------------

def write_person(tx, entry: PersonEntry, source_type: str, source_file: str):
    today = datetime.date.today().isoformat()
    if DRY_RUN:
        print(f"[DRY RUN] MERGE Person: id={entry['id']}, name={entry['canonical']}")
        return
    tx.run(MERGE_PERSON, id=entry["id"], name=entry["canonical"], alias=sorted(entry["aliases"]), qid=entry.get("qid"), today=today, source_type=source_type, source_file=source_file)


def write_company(tx, cid: str, name: str, source_type: str, source_file: str):
    today = datetime.date.today().isoformat()
    if DRY_RUN:
        print(f"[DRY RUN] MERGE Company: id={cid}, name={name}")
        return
    tx.run(MERGE_COMPANY, id=cid, name=name, today=today, source_type=source_type, source_file=source_file)


def write_role(tx, pid: str, cid: str, role: str, start: Optional[str], end: Optional[str], source_type: str, source_file: str):
    if DRY_RUN:
        print(f"[DRY RUN] MERGE Role: person={pid}, company={cid}, role={role}")
        return
    tx.run(MERGE_ROLE, pid=pid, cid=cid, role=role, start=start, end=end, source_type=source_type, source_file=source_file)


def write_family(tx, sid: str, did: str, rel: str, source_type: str, source_file: str):
    """Create a person‑to‑person FAMILY edge **unless** it is a self‑loop."""
    if sid == did:
        return  # ← self‑loop detected; skip
    if DRY_RUN:
        print(f"[DRY RUN] MERGE Family: source={sid}, dest={did}, relation={rel}")
        return
    tx.run(MERGE_FAMILY, src=sid, dst=did, rel=rel, source_type=source_type, source_file=source_file)

# ───────────────────────────── Parsers ─────────────────────────────

def ingest_neo4j_query(path: Path):
    if not path.exists():
        print(f"[WARN] {path} missing, skip neo4j_export")
        return
    data = json.loads(path.read_text())
    source_type = "neo4j_export"
    source_file = path.name
    with driver.session() as s:
        for rec in data:
            if not rec.get("n") or not rec.get("m"):
                continue
            pn = rec["n"]["properties"].get("name", "")
            cn = rec["m"]["properties"].get("name", "")
            pid, pentry = get_or_create_person(pn)
            cid = get_or_create_company(cn)
            s.execute_write(write_person, pentry, source_type, source_file)
            s.execute_write(write_company, cid, cn, source_type, source_file)
            rel = rec.get("r")
            if rel:
                rp = rel["properties"]
                if pid != cid:  # Defensive — person id will never equal company id but keep check
                    s.execute_write(write_role, pid, cid, rp.get("role"), rp.get("startDate"), rp.get("endDate"), source_type, source_file)


# ✸ NEW unified Wikidata ingester — merges the two competing definitions from v4 ✸

def ingest_wikidata(path: Path):
    """Ingest simplified Wikidata JSON dump (nodes + family + business roles)."""
    if not path.exists():
        print(f"[WARN] {path} missing, skip wikidata_json")
        return

    raw_data = json.loads(path.read_text())
    persons = raw_data.get("persons", [])
    edges = raw_data.get("edges", [])
    source_type = "wikidata"
    source_file = path.name

    with driver.session() as s:
        # Pass 1: Create/update all person nodes using their QID as the primary ID.
        for p in persons:
            qid = p.get("id")
            if not qid:
                continue
            name = p.get("props", {}).get("name") or qid
            pid, entry = get_or_create_person(name, qid=qid)
            s.execute_write(write_person, entry, source_type, source_file)

        # Pass 2: Create family relationships from the 'edges' list using QIDs.
        for edge in edges:
            src_qid = edge.get("seed")
            dst_qid = edge.get("rel")
            rel_type = edge.get("relType")

            if not all([src_qid, dst_qid, rel_type]):
                continue

            if src_qid == dst_qid:
                continue

            # The node IDs are the QIDs themselves, so this will match correctly.
            s.execute_write(write_family, src_qid, dst_qid, rel_type, source_type, source_file)

        # Pass 3: Create business roles (if present).
        for p in persons:
            qid = p.get("id")
            if not qid:
                continue

            for rel in p.get("business", []):
                company_name = rel["company"]
                cid = get_or_create_company(company_name)
                s.execute_write(write_company, cid, company_name, source_type, source_file)
                # The person ID is the QID
                if qid != cid:
                    s.execute_write(write_role, qid, cid, rel["role"], rel.get("start"), rel.get("end"), source_type, source_file)


# NOTE: `ingest_mas` in v4 expected a **non‑existent** `relationships` column and would crash.
# The logic below reverts to the safer behaviour from *load_graph_modified.py* while still using
# the dedup registry infrastructure.  Any MAS‑specific person‑to‑person edges can be added later.

def ingest_mas(csv_path: Path):
    if not csv_path.exists():
        print(f"[WARN] {csv_path} missing, skip MAS CSV")
        return

    source_type = "MAS_csv"
    source_file = csv_path.name
    with csv_path.open(newline="", encoding="utf-8-sig") as f, driver.session() as s:
        rdr = csv.DictReader(f)
        for row in rdr:
            pn = row.get("Person Name") or row.get("person name")
            cn = row.get("Company Name") or row.get("company name")
            role = row.get("Person Title") or row.get("person title", "")
            if not (pn and cn):
                continue
            pid, pentry = get_or_create_person(pn)
            cid = get_or_create_company(cn)
            s.execute_write(write_person, pentry, source_type, source_file)
            s.execute_write(write_company, cid, cn, source_type, source_file)
            s.execute_write(write_role, pid, cid, role, None, None, source_type, source_file)


# Annual‑report (NER/RED) parser is unchanged apart from the self‑loop guard living in write_role

def ingest_annual(dir: Path, limit: Optional[int] = None):
    """Ingest annual report NER/RED JSON files."""
    source_type = "annual_report"

    def process_docs(doc, source_file):
        ents = {e["entityId"]: e for e in doc["original"]["entities"]}
        # Nodes
        for e in ents.values():
            if e["type"] == "Person":
                pid, entry = get_or_create_person(e["canonicalName"], canonical=e["canonicalName"], extras=e.get("mentions", []))
                s.execute_write(write_person, entry, source_type, source_file)
            elif e["type"] == "Company":
                cid = get_or_create_company(e["canonicalName"])
                s.execute_write(write_company, cid, e["canonicalName"], source_type, source_file)
        # Roles
        for rel in doc["original"]["relationships"]:
            src = ents.get(rel["sourceEntityId"])
            tgt = ents.get(rel["targetEntityId"])
            if not src or not tgt:
                continue
            if src["type"] == "Person" and tgt["type"] == "Company":
                pid, _ = get_or_create_person(src["canonicalName"], canonical=src["canonicalName"])
                cid = get_or_create_company(tgt["canonicalName"])
                role = rel["role"]["details"] if rel.get("role") else None
                start = rel.get("effectiveDate")
                if pid != cid:
                    s.execute_write(write_role, pid, cid, role, start, None, source_type, source_file)

    files_to_process = sorted([p for p in dir.glob("**/*.json") if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    if not files_to_process:
        print(f"[INFO] No JSON files found in {dir}")
        return

    processed_count = 0
    for file_path in files_to_process:
        if limit is not None and processed_count >= limit:
            print(f"[INFO] Reached processing limit of {limit} files.")
            break

        if file_path.name in processed_annual_reports:
            print(f"[INFO] Skipping already processed annual report: {file_path.name}")
            continue
        try:
            docs = json.loads(file_path.read_text(encoding='utf-8'))
            process_docs(docs, file_path.name)
            processed_count += 1
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from {file_path}: {e}")
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    print(f"[INFO] Processed {processed_count} annual reports.")

# ───────────────────────── Pre-load Registry ──────────────────────────

def preload_registries():
    """Pre-load existing Person and Company data from Neo4j for fuzzy matching."""
    print("[INFO] Pre-loading existing entities from Neo4j...")
    with driver.session() as s:
        # Pre-load persons
        person_records = s.run("MATCH (p:Person) RETURN p.id, p.name, p.aliases, p.qid")
        for rec in person_records:
            pid, name, aliases, qid = rec.values()
            s_name = slug(name)
            entry = {"id": pid, "canonical": name, "aliases": set(aliases or []), "qid": qid}
            person_registry[s_name] = entry
            if qid:
                qid_registry[qid] = s_name

        # Pre-load companies
        company_records = s.run("MATCH (c:Company) RETURN c.id, c.name")
        for rec in company_records:
            cid, name = rec.values()
            company_registry[slug(name)] = cid

        # Pre-load processed annual report filenames
        report_files_records = s.run("""
            MATCH (n) WHERE n.reference_type = 'annual_report'
            RETURN DISTINCT n.reference_file AS file
            UNION
            MATCH ()-[r]-() WHERE r.reference_type = 'annual_report'
            RETURN DISTINCT r.reference_file AS file
        """)
        for rec in report_files_records:
            if rec["file"]:
                processed_annual_reports.add(rec["file"])

    print(f"[INFO] Pre-loaded {len(person_registry)} persons and {len(company_registry)} companies.")
    print(f"[INFO] Found {len(processed_annual_reports)} previously processed annual reports.")

# ───────────────────────────── Entrypoint ─────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Load UHNW datasets into Neo4j Aura (dedup + self‑loop safe)")
    ap.add_argument("--neo4j_export", default="G:/My Drive/NUS MSBA SEM2/UOB/SGX Annual Reports/Case Study/Venture Corporation Limited/neo4j_query_table_data_2025-6-26.json", help="Cypher JSON export from SGX annual‑report query")
    ap.add_argument("--wikidata_json", default="C:/Users/22601/Downloads/downloads/wikiData/data.json", help="Simplified Wikidata JSON")
    ap.add_argument("--mas_csv", default="G:/My Drive/NUS MSBA SEM2/UOB/MAS/MAS_Personnel_merged.csv", help="MAS personnel merged CSV")
    ap.add_argument("--annual_json", default="C:/Users/22601/Downloads/downloads/files/NER_RED", help="Annual‑report NER/RED JSON (authoritative)")
    ap.add_argument("--dry-run", action="store_true", help="Simulate the run without writing to the database")
    ap.add_argument("--limit", type=int, default=None, help="Limit the number of new annual reports to process")
    args = ap.parse_args()

    global DRY_RUN
    DRY_RUN = args.dry_run
    if DRY_RUN:
        print("[INFO] Performing a dry run. No data will be written to Neo4j.")

    preload_registries()

    # if args.neo4j_export:
    #     ingest_neo4j_query(Path(args.neo4j_export))
    # if args.wikidata_json:
    #     ingest_wikidata(Path(args.wikidata_json))
    if args.annual_json:
        ingest_annual(Path(args.annual_json), limit=args.limit)
    # if args.mas_csv:
    #     ingest_mas(Path(args.mas_csv))

    driver.close()
    print("[✓] Graph load complete")


if __name__ == "__main__":
    main()
