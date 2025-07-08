
"""
sgx_ner_to_neo4j.py
-------------------

End‑to‑end pipeline to extract People‑↔︎Organisation role relations from an SGX
annual‑report PDF and push them into Neo4j Aura using Google Gemini 2.5 Pro for
NER.

⚙️  Requirements
    pip install google-generativeai pdfplumber python-dotenv langchain py2neo

The script expects these **environment variables** (e.g. in a `.env` file):

    GOOGLE_API_KEY      # your Google AI Developer key
    NEO4J_URI           # e.g. neo4j+s://a0183311.databases.neo4j.io
    NEO4J_USERNAME      # neo4j
    NEO4J_PASSWORD      # 40‑char secret from Aura
    PDF_PATH            # path to local annual‑report PDF
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from typing import Dict, List

import pdfplumber                         # PDF text extraction
from dotenv import load_dotenv            # env helper
from google import generativeai as genai  # Gemini 2.5 API
from langchain.text_splitter import RecursiveCharacterTextSplitter
from py2neo import Graph, Node, Relationship

# --------------------------------------------------------------------------- #
# ----------------------------  CONFIGURATION  ------------------------------ #
# --------------------------------------------------------------------------- #

load_dotenv()                             # loads .env if present

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
PDF_PATH       = os.getenv("PDF_PATH", "annual_report.pdf")
CHUNK_SIZE     = int(os.getenv("CHUNK_SIZE", 3000))
CHUNK_OVERLAP  = int(os.getenv("CHUNK_OVERLAP", 250))
MODEL_NAME     = os.getenv("GEMINI_MODEL", "gemini-2.5-pro")

NEO4J_URI      = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_CLEAR    = os.getenv("NEO4J_CLEAR", "false").lower() == "true"

for var in ("GOOGLE_API_KEY", "NEO4J_URI", "NEO4J_PASSWORD"):
    if not globals()[var]:
        raise EnvironmentError(f"Missing required env var: {var}")

genai.configure(api_key=GOOGLE_API_KEY)
gemini = genai.GenerativeModel(MODEL_NAME)

# --------------------------------------------------------------------------- #
# ---------------------------  PDF HELPERS  --------------------------------- #
# --------------------------------------------------------------------------- #

def extract_text_from_pdf(path: str) -> str:
    """Return concatenated text from every page of a PDF."""
    text_parts = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text_parts.append(page_text)
    return "\n".join(text_parts)


def chunk_text(text: str,
               chunk_size: int = CHUNK_SIZE,
               overlap: int = CHUNK_OVERLAP) -> List[str]:
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        separators=["\n\n", "\n", " ", ""]
    )
    return splitter.split_text(text)

# --------------------------------------------------------------------------- #
# --------------------------  GEMINI  NER  ---------------------------------- #
# --------------------------------------------------------------------------- #

SYSTEM_PROMPT = """You are a financial-domain NER engine.
Given a chunk from an SGX-listed company's annual report,
identify every PERSON–COMPANY role relationship described.
Return ONLY JSON Lines, one line per relation, with keys:
    person   : string  (names as they appear)
    role     : string  (job title or board position, lowercase)
    company  : string  (organisation name)
If none found, return an empty JSON object {{}} on one line.
Do NOT output anything else.
"""

def ner_chunk(chunk: str) -> List[Dict[str, str]]:
    response = gemini.generate_content(
        contents=[{"role": "system", "parts": [SYSTEM_PROMPT]},
                  {"role": "user",   "parts": [chunk]}]
    )
    # Model returns a blob of lines; filter and parse JSON
    relations = []
    for line in response.text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if obj:
                relations.append(obj)
        except json.JSONDecodeError:
            # try to salvage with a greedy regex
            match = re.search(r"{.*}", line)
            if match:
                try:
                    relations.append(json.loads(match.group(0)))
                except Exception:
                    pass
    return relations

# --------------------------------------------------------------------------- #
# ------------------------  NEO4J  LOADER  ---------------------------------- #
# --------------------------------------------------------------------------- #

def push_to_neo4j(relations: List[Dict[str, str]]) -> None:
    graph = Graph(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    if NEO4J_CLEAR:
        graph.run("MATCH (n) DETACH DELETE n")

    node_cache = {}  # (label, name) -> Node

    for rel in relations:
        person = rel.get("person")
        role   = rel.get("role")
        company= rel.get("company")

        if not all((person, role, company)):
            continue

        key_person = ("Person", person)
        key_comp   = ("Company", company)

        if key_person not in node_cache:
            node = Node("Person", name=person)
            graph.merge(node, "Person", "name")
            node_cache[key_person] = node

        if key_comp not in node_cache:
            node = Node("Company", name=company)
            graph.merge(node, "Company", "name")
            node_cache[key_comp] = node

        rel_obj = Relationship(
            node_cache[key_person],
            "HAS_ROLE_AT",
            node_cache[key_comp],
            role=role
        )
        graph.merge(rel_obj)

# --------------------------------------------------------------------------- #
# ------------------------------  MAIN  ------------------------------------- #
# --------------------------------------------------------------------------- #

def main() -> None:
    print("📖 Extracting text from", PDF_PATH)
    raw_text = extract_text_from_pdf(PDF_PATH)

    print("✂️  Splitting into chunks ...")
    chunks = chunk_text(raw_text)
    print(f"   → {len(chunks)} chunks to process")

    all_relations: List[Dict[str, str]] = []
    for i, chunk in enumerate(chunks, 1):
        print(f"🧠  Gemini NER on chunk {i}/{len(chunks)}", end="\r")
        rels = ner_chunk(chunk)
        all_relations.extend(rels)

    print("\n🔗 Extracted", len(all_relations), "person–company relations")
    if not all_relations:
        return

    print("🚚 Loading into Neo4j ...")
    push_to_neo4j(all_relations)
    print("✅ Done. View graph at", NEO4J_URI)

if __name__ == "__main__":
    main()
