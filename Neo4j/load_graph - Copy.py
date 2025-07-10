#!/usr/bin/env python3
"""
Load three UHNW‑related data sources into a Neo4j Aura instance using the schema above.

Usage:
  export NEO4J_URI='neo4j+s://<your-aura-endpoint>'
  export NEO4J_USER='neo4j'
  export NEO4J_PASSWORD='<password>'
  python load_graph.py \
      --neo4j_export neo4j_query_table_data_2025-6-26.json \
      --wikidata_json data.json \
      --mas_personnel_csv MAS_first_1500.json

n8n v1.98.1 tip: wrap this script with an **Execute Command** node (Image: `python:3.12-alpine`) and mount the dataset volume for fully automated nightly refresh.
"""

import os, json, csv, argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
from neo4j import GraphDatabase

# ──────────────────────────────────────────────────────────────────────────────
# Neo4j connection helpers
# ──────────────────────────────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USER     = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

MERGE_PERSON = """
MERGE (p:Person {id:$id})
SET   p.name         = COALESCE(p.name, $name),
      p.lastUpdated  = datetime($lastUpdated),
      p += $props
"""

MERGE_COMPANY = """
MERGE (c:Company {id:$id})
SET   c.name        = COALESCE(c.name, $name),
      c.lastUpdated = datetime($lastUpdated),
      c += $props
"""

MERGE_ROLE = """
MATCH (p:Person {id:$person_id})
MATCH (c:Company {id:$company_id})
MERGE (p)-[r:HAS_ROLE_AT {role:$role}]->(c)
SET   r.startDate = $startDate,
      r.endDate   = $endDate,
      r.source    = $source
"""

MERGE_FAMILY = """
MATCH (a:Person {id:$src_id})
MATCH (b:Person {id:$dst_id})
MERGE (a)-[r:FAMILY {relation:$relation}]->(b)
SET   r.source = $source
"""

def merge_person(tx, id:str, name:str, props:Dict[str,Any]):
    tx.run(MERGE_PERSON,
           id=id,
           name=name,
           props={k:v for k,v in props.items() if v is not None},
           lastUpdated=os.getenv('LOAD_TS','2025-07-08'))

def merge_company(tx, id:str, name:str, props:Dict[str,Any]):
    tx.run(MERGE_COMPANY,
           id=id,
           name=name,
           props={k:v for k,v in props.items() if v is not None},
           lastUpdated=os.getenv('LOAD_TS','2025-07-08'))

def merge_role(tx, person_id:str, company_id:str, role:str, start:Optional[int], end:Optional[int], source:str):
    tx.run(MERGE_ROLE,
           person_id=person_id,
           company_id=company_id,
           role=role,
           startDate=start,
           endDate=end,
           source=source)

def merge_family(tx, src_id:str, dst_id:str, relation:str, source:str):
    tx.run(MERGE_FAMILY,
           src_id=src_id,
           dst_id=dst_id,
           relation=relation,
           source=source)

# ──────────────────────────────────────────────────────────────────────────────
# Parsers for each dataset
# ──────────────────────────────────────────────────────────────────────────────

def ingest_neo4j_query_table(path:Path):
    """Ingest `neo4j_query_table_data_2025-6-26.json`."""
    data = json.loads(path.read_text(encoding='utf-8'))
    with driver.session() as session:
        for record in data:
            p   = record['n']
            c   = record['m']
            rel = record['r']
            pid = f"src1:{p['identity']}"
            cid = f"src1:{c['identity']}"
            session.write_transaction(merge_person,  pid, p['properties']['name'], p['properties'])
            session.write_transaction(merge_company, cid, c['properties']['name'], c['properties'])
            if rel:
                rp = rel['properties']
                session.write_transaction(merge_role,
                                          pid,
                                          cid,
                                          rp.get('role'),
                                          rp.get('startDate'),
                                          rp.get('endDate'),
                                          'neo4j_query_table_data')

def ingest_wikidata_json(path:Path):
    """Ingest simplified Wikidata‑style JSON (`data.json`)."""
    persons = json.loads(path.read_text(encoding='utf-8'))['persons']
    with driver.session() as session:
        # pass 1 – nodes
        for p in persons:
            pid = f"wd:{p['id']}"
            session.write_transaction(merge_person, pid, p['props']['name'], p['props'])
        # pass 2 – family relationships
        for p in persons:
            src_id = f"wd:{p['id']}"
            attrs  = p.get('attributes', {})
            for tag,label in attrs.items():
                if not tag.endswith('Label'):
                    continue
                relation = tag.replace('Label','').lower()  # fatherLabel → father
                targets  = label if isinstance(label, list) else [label]
                for name in targets:
                    dst_id = f"wd:label:{name}"
                    session.write_transaction(merge_person, dst_id, name, {})
                    session.write_transaction(merge_family, src_id, dst_id, relation, 'wikidata_json')

# NOTE: MAS_first_1500.json is an n8n workflow *definition* rather than flat data.
# In production we recommend exporting the scraped personnel rows into CSV/JSON.
# Below is a stub that expects such a CSV (company_name,person_name,person_title).

def ingest_mas_csv(csv_path:Path):
    with driver.session() as session, csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = f"mas:{row['person_name']}".lower().replace(' ','_')
            cid = f"mas:{row['company_name']}".lower().replace(' ','_')
            session.write_transaction(merge_person,  pid, row['person_name'], {})
            session.write_transaction(merge_company, cid, row['company_name'], {})
            session.write_transaction(merge_role, pid, cid, row['person_title'], None, None, 'MAS_scrape')

# ──────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────────────────────

def main(args):
    ingest_neo4j_query_table(Path(args.neo4j_export))
    ingest_wikidata_json(Path(args.wikidata_json))
    ingest_mas_csv(Path(args.mas_personnel_csv))
    driver.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Load UHNW datasets into Neo4j Aura")
    p.add_argument("--neo4j_export",     required=True)
    p.add_argument("--wikidata_json",    required=True)
    p.add_argument("--mas_personnel_csv",required=True,
                   help="Flattened MAS personnel file (generated by your n8n workflow)")
    main(p.parse_args())