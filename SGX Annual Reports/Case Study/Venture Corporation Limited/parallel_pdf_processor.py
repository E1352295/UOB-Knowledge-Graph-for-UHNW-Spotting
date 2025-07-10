# enhanced_pdf_processor.py (v9 - Improved Personnel Filtering)

import pandas as pd
import logging
import re
import sys
import json
from pathlib import Path
from pypdf import PdfReader
from typing import List, Optional, Dict, Any, Set
import concurrent.futures
import os
from tqdm import tqdm
import spacy

# --- Logging Configuration ---
def setup_logging():
    """Detects the environment and configures logging accordingly."""
    try:
        # Check for IPython kernel, which is used by Jupyter
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        if 'ZMQInteractiveShell' in shell:
            # Jupyter notebook or qtconsole
            print("Running in a notebook environment. Configuring logger for stdout.")
            logger = logging.getLogger()
            # Remove existing handlers to avoid duplicate messages in notebooks
            if logger.handlers:
                for handler in logger.handlers[:]: # Iterate over a copy
                    logger.removeHandler(handler)
            logging.basicConfig(
                level=logging.INFO, 
                format='%(asctime)s - %(levelname)s - %(message)s',
                stream=sys.stdout, # Force output to cell
                force=True # Override any existing root logger config
            )
            return
    except (NameError, ImportError):
        # Not in an IPython environment
        pass

    # Standard script execution
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

setup_logging() # Initialize logging

# --- NLP Model Loading ---
try:
    NLP = spacy.load("en_core_web_sm", disable=["parser", "lemmatizer"])
except OSError:
    logging.error("spaCy model 'en_core_web_sm' not found.")
    logging.error("Please run: python -m spacy download en_core_web_sm")
    sys.exit(1)

# --- Enhanced Regex Patterns ---
BKMK_PATTERNS = re.compile(
    r"(board\s+of\s+directors|directors'\s+profile|directors\b|management\s+team|key\s+management|executive\s+team|key\s+personnel|corporate\s+governance)",
    re.IGNORECASE,
)

# Personnel role indicators
ROLE_PATTERNS = re.compile(
    r"\b(director|chairman|chairwoman|chairperson|ceo|cfo|coo|cto|president|vice\s+president|vp|executive|manager|head\s+of|chief|lead|senior|principal|partner)\b",
    re.IGNORECASE
)

# Relationship indicators
RELATIONSHIP_PATTERNS = re.compile(
    r"\b(appointed|serves?\s+as|holds?\s+the\s+position|responsible\s+for|oversees|leads|manages|reports?\s+to|works?\s+with|joined|prior\s+to|previously|experience\s+at|worked\s+at|formerly)\b",
    re.IGNORECASE
)

# Exclude patterns for irrelevant content
EXCLUDE_PATTERNS = re.compile(
    r"\b(financial\s+highlights|revenue|profit|loss|earnings|balance\s+sheet|cash\s+flow|dividend|share\s+price|market\s+cap|auditor|accounting|footnote|note\s+\d+|schedule|appendix|index|table\s+of\s+contents)\b",
    re.IGNORECASE
)

YEAR_RE = re.compile(r"\b(20\d{2})\b")
FY_YEAR_PATTERNS = [re.compile(r"financial\s+year\s+.*?(20\d{2})", re.I), re.compile(r"FY\s*(\d{2,4})", re.I)]

# --- Heuristics for Confidence Scoring ---
CONFIDENCE_RULES = {"annual report": 50, "financial statements": 20, "sgx": 15, "agm": 10, "interim report": -60, "prospectus": -50}

# --- Enhanced Filtering Functions ---

def extract_person_names(doc) -> Set[str]:
    """Extract unique person names, filtering out common false positives."""
    persons = set()
    false_positives = {'singapore', 'malaysia', 'china', 'asia', 'group', 'limited', 'ltd', 'pte', 'corp', 'inc', 'corporation'}
    
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            name = ent.text.strip().lower()
            # Filter out obvious false positives
            if len(name) > 2 and not any(fp in name for fp in false_positives):
                persons.add(ent.text.strip())
    
    return persons

def extract_organizations(doc) -> Set[str]:
    """Extract organization names."""
    orgs = set()
    for ent in doc.ents:
        if ent.label_ == "ORG":
            org_name = ent.text.strip()
            if len(org_name) > 2:
                orgs.add(org_name)
    return orgs

def extract_main_company_from_early_pages(text: str, nlp_model) -> Optional[str]:
    """Extract the main company name from the text of the first few pages."""
    if not text:
        return None

    doc = nlp_model(text)
    org_entities = [ent.text.strip() for ent in doc.ents if ent.label_ == 'ORG']

    if not org_entities:
        return None

    # Heuristics to find the most likely company name
    from collections import Counter
    # Filter out short names and generic terms
    candidate_orgs = [
        org for org in org_entities 
        if len(org) > 3 and 'group' not in org.lower()
    ]

    if not candidate_orgs:
        return None

    # Prioritize names with corporate suffixes
    suffixes = ['limited', 'ltd', 'corporation', 'inc', 'bhd']
    for org in candidate_orgs:
        if any(suffix in org.lower() for suffix in suffixes):
            return org

    # Fallback to the most common organization name
    most_common_org = Counter(candidate_orgs).most_common(1)[0][0]
    return most_common_org

def has_personnel_context(text: str) -> bool:
    """Check if text contains personnel-related context indicators."""
    # Look for role indicators
    role_matches = len(ROLE_PATTERNS.findall(text))
    
    # Look for relationship indicators
    relationship_matches = len(RELATIONSHIP_PATTERNS.findall(text))
    
    # Personnel context score
    context_score = role_matches + relationship_matches
    
    return context_score >= 2  # Lowered from 3 to be less strict

def has_exclusion_content(text: str) -> bool:
    """Check if text contains content that should be excluded."""
    return bool(EXCLUDE_PATTERNS.search(text))

def calculate_personnel_density(text: str, persons: Set[str], orgs: Set[str]) -> float:
    """Calculate the density of personnel-related content."""
    if not text:
        return 0.0
    
    word_count = len(text.split())
    if word_count < 50:  # Too short to be meaningful
        return 0.0
    
    # Count mentions of persons and organizations
    text_lower = text.lower()
    person_mentions = sum(text_lower.count(person.lower()) for person in persons)
    org_mentions = sum(text_lower.count(org.lower()) for org in orgs)
    
    # Calculate density (mentions per 100 words)
    density = ((person_mentions + org_mentions) / word_count) * 100
    
    return density

def has_relevant_personnel_content(page_text: str, nlp_model) -> bool:
    """
    Enhanced filtering for personnel-related content with multiple criteria:
    1. Must have sufficient named entities (persons/orgs)
    2. Must have personnel context indicators (roles, relationships)
    3. Must not contain excluded financial content
    4. Must have good personnel content density
    """
    if not page_text or len(page_text) < 200:  # Increased minimum length
        return False
    
    # Check for exclusion patterns first
    if has_exclusion_content(page_text):
        logging.debug("Page rejected: Contains excluded financial content")
        return False
    
    # Process with NLP
    doc = nlp_model(page_text)
    persons = extract_person_names(doc)
    orgs = extract_organizations(doc)
    
    # Criterion 1: Must have sufficient entities
    entity_threshold_met = (
        len(persons) >= 2 or  # Lowered from 3
        (len(persons) >= 1 and len(orgs) >= 1)  # Lowered from 2/2
    )
    
    if not entity_threshold_met:
        logging.debug(f"Page rejected: Insufficient entities (persons: {len(persons)}, orgs: {len(orgs)})")
        return False
    
    # Criterion 2: Must have personnel context
    if not has_personnel_context(page_text):
        logging.debug("Page rejected: No personnel context indicators")
        return False
    
    # Criterion 3: Must have good personnel content density
    density = calculate_personnel_density(page_text, persons, orgs)
    if density < 1.5:  # Lowered from 2.0, less than 1.5 entity mentions per 100 words
        logging.debug(f"Page rejected: Low density ({density:.2f})")
        return False
    
    logging.debug(f"Page accepted: persons={len(persons)}, orgs={len(orgs)}, density={density:.2f}")
    return True

def analyze_report_metadata(text_corpus: str, filename: str) -> Dict[str, Any]:
    filename_lower = filename.lower()
    report_year = None
    confidence = -1

    # Priority 1: Check filename for definitive clues
    fn_year_match = YEAR_RE.search(filename)
    if fn_year_match:
        report_year = fn_year_match.group(0)

    if "annual" in filename_lower:
        confidence = 100

    # Priority 2: Analyze text if needed
    text_lower = text_corpus.lower()

    if not report_year:
        for pattern in FY_YEAR_PATTERNS:
            match = pattern.search(text_lower)
            if match:
                year_str = match.group(1)
                report_year = f"20{year_str}" if len(year_str) == 2 else year_str
                break

    if confidence == -1:
        score = sum(points for keyword, points in CONFIDENCE_RULES.items() if keyword in text_lower)
        confidence = max(0, min(100, 50 + score))

    return {
        "report_year": report_year or "UNKNOWN",
        "ar_confidence": int(confidence)
    }

def extract_board_pages_and_content(reader: PdfReader, pdf_path: Path) -> tuple[List[int], Dict[int, str]]:
    pdf_name_for_logging = pdf_path.name
    candidate_pages = {}

    # Step 1: Gather candidate pages from bookmarks and full-text search
    all_bookmarks = []
    def _flatten_bookmarks(items):
        for item in items:
            if isinstance(item, list): 
                _flatten_bookmarks(item)
            else:
                try:
                    page_num = reader.get_destination_page_number(item)
                    title = getattr(item, "title", "")
                    if title and page_num is not None: 
                        all_bookmarks.append({'title': str(title), 'page': page_num})
                except Exception: 
                    pass
    
    try:
        _flatten_bookmarks(reader.outline)
        if all_bookmarks:
            all_bookmarks.sort(key=lambda x: x['page'])
            unique_bms = [bm for i, bm in enumerate(all_bookmarks) if i == 0 or bm['page'] != all_bookmarks[i-1]['page']]
            for i, bm in enumerate(unique_bms):
                if BKMK_PATTERNS.search(bm['title']):
                    start_page = bm['page']
                    end_page = unique_bms[i + 1]['page'] if i + 1 < len(unique_bms) else len(reader.pages)
                    for page_idx in range(start_page, end_page):
                        if page_idx not in candidate_pages:
                            try: 
                                candidate_pages[page_idx] = reader.pages[page_idx].extract_text() or ""
                            except Exception: 
                                pass
    except Exception: 
        pass

    # Fallback: full-text search if no bookmarks found
    if not candidate_pages:
        for i, page in enumerate(reader.pages):
            try:
                text = page.extract_text()
                if text and BKMK_PATTERNS.search(text): 
                    candidate_pages[i] = text
            except Exception: 
                continue

    # Step 2: Enhanced filtering using personnel content analysis
    final_pages_content = {}
    for page_idx, text in candidate_pages.items():
        if has_relevant_personnel_content(text, NLP):
            final_pages_content[page_idx] = text
            logging.info(f"Page {page_idx + 1} accepted for '{pdf_name_for_logging}' (personnel content validated).")
        else:
            logging.info(f"Page {page_idx + 1} rejected for '{pdf_name_for_logging}' (insufficient personnel content).")
    
    if not final_pages_content:
        logging.warning(f"Warning ({pdf_name_for_logging}): No pages with relevant personnel content found.")
        return [], {}

    return sorted(final_pages_content.keys()), final_pages_content

def extract_company_name(pdf_path: Path) -> str:
    if pdf_path.parent.name != pdf_path.root and pdf_path.parent.name.lower() not in ['pdfs', 'documents', 'files']:
        return pdf_path.parent.name
    return pdf_path.stem

def save_extracted_content(pdf_path: Path, page_content_dict: Dict[int, str], metadata_extras: Dict[str, Any], json_file_path: Path) -> Optional[Path]:
    """Saves the extracted page content and metadata to a JSON file."""
    if not page_content_dict: 
        return None
    try:
        # Ensure the target directory exists before writing
        json_file_path.parent.mkdir(exist_ok=True)

        metadata = {
            "source_pdf": pdf_path.name, 
            "extraction_date_utc": pd.Timestamp.now(tz='UTC').isoformat(), 
            "processor_script": Path(sys.argv[0]).name, 
            "total_pages_extracted": len(page_content_dict), 
            **metadata_extras
        }
        content = [{"page_number": i + 1, "text": t} for i, t in sorted(page_content_dict.items())]
        final_json_data = {"metadata": metadata, "content": content}
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(final_json_data, f, ensure_ascii=False, indent=2)
        return json_file_path
    except Exception as e:
        logging.error(f"Failed to save extracted content for {pdf_path.name}: {e}")
        return None

def process_pdf_file(pdf_path: Path) -> Optional[Dict[str, Any]]:
    # --- Pre-computation Check: Skip if output already exists ---
    try:
        extracted_content_dir = pdf_path.parent / "ExtractedContent"
        json_filename = pdf_path.stem + "_extracted.json"
        json_file_path = extracted_content_dir / json_filename

        if json_file_path.exists():
            logging.info(f"Skipping '{pdf_path.name}' as its output file already exists.")
            return None  # Skip processing entirely
    except Exception as e:
        # This check should not fail, but if it does, log it and continue
        logging.warning(f"Pre-check failed for {pdf_path.name}: {e}")

    try:
        reader = PdfReader(pdf_path)
        # Read first 4 pages for metadata and company name extraction
        early_pages_text = "".join((reader.pages[i].extract_text() or "") + "\n" for i in range(min(4, len(reader.pages))))
        report_meta = analyze_report_metadata(early_pages_text, pdf_path.name)

        # Extract company name from early pages, fallback to old method
        company_name = extract_main_company_from_early_pages(early_pages_text, NLP)
        if not company_name:
            company_name = extract_company_name(pdf_path)

        # if report_meta.get("report_year") != "2024":
        #     logging.info(f"Skipping ({pdf_path.name}): Report year is '{report_meta.get('report_year')}', not 2024.")
        #     return None

        board_pages_indices, page_content_dict = extract_board_pages_and_content(reader, pdf_path)
        
        if not board_pages_indices:
            logging.info(f"Skipped ({pdf_path.name}): No relevant personnel pages found in this report.")
            return None
        
        saved_path = save_extracted_content(pdf_path, page_content_dict, report_meta, json_file_path)
        total_content_length = sum(len(content) for content in page_content_dict.values())
        
        return {
            'file_name': pdf_path.name, 
            'company_name': company_name,
            'report_year': report_meta['report_year'], 
            'ar_confidence': report_meta['ar_confidence'],
            'board_pages': ','.join(map(str, [p + 1 for p in board_pages_indices])),
            'total_board_pages': len(board_pages_indices),
            'extracted_json_file': saved_path.name if saved_path else None,
            'content_length': total_content_length
        }
    except Exception as e:
        logging.error(f"Failed ({pdf_path.name}): Critical error: {e}")
        return None

def main(root_dir_str: str, use_parallel: bool = True):
    root_dir = Path(root_dir_str)
    pdf_files = list(root_dir.rglob("*.pdf"))
    if not pdf_files:
        logging.error(f"No PDF files found in directory: {root_dir}")
        return
    
    logging.info(f"Found {len(pdf_files)} PDF files. Processing...")
    results = []
    
    if use_parallel and len(pdf_files) > 1:
        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_pdf = {executor.submit(process_pdf_file, pdf): pdf for pdf in pdf_files}
            for future in tqdm(concurrent.futures.as_completed(future_to_pdf), total=len(pdf_files), desc="Processing PDFs"):
                try:
                    result = future.result()
                    if result: 
                        results.append(result)
                except Exception as exc:
                    logging.error(f'Subprocess task failed for {future_to_pdf[future].name}: {exc}')
    else:
        for pdf_file in tqdm(pdf_files, desc="Processing PDFs"):
            result = process_pdf_file(pdf_file)
            if result: 
                results.append(result)
    
    logging.info("All processing tasks completed.")
    
    if results:
        df_results = pd.DataFrame(results)
        cols_order = ['file_name', 'company_name', 'report_year', 'ar_confidence', 'total_board_pages', 'board_pages', 'content_length', 'extracted_json_file']
        df_results = df_results[[col for col in cols_order if col in df_results.columns]]
        output_csv_path = root_dir.parent / f"{root_dir.name}_extraction_results.csv"
        df_results.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        logging.info(f"Results saved to: {output_csv_path}")
        print("\n--- Processing Summary ---")
        print(f"Total PDFs processed: {len(pdf_files)}")
        print(f"Successful extractions: {len(results)}")
        print(f"Success rate: {len(results)/len(pdf_files)*100:.1f}%")
        print("\n--- Sample Results ---")
        print(df_results.head(10).to_string(index=False))
    else:
        logging.warning("No successful page extractions from any PDF files.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        directory_to_process = sys.argv[1]
        use_parallel = "--sequential" not in sys.argv
        main(directory_to_process, use_parallel=use_parallel)
    else:
        print("Error: Please provide the directory path to process.")
        print(f"Usage: python {sys.argv[0]} \"<directory_path>\" [--sequential]")
        sys.exit(1)