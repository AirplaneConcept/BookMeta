"""
BookMeta - Personal Book Metadata Authority
A local web app that scans your ebook library, extracts ISBNs,
queries OpenLibrary/Google Books, and stores authoritative metadata in SQLite.
"""

import os
import re
import json
import sqlite3
import zipfile
import threading
import time
from pathlib import Path
from flask import Flask, render_template, request, jsonify, g

# ─── Optional PDF support ───────────────────────────────────────────────────
try:
    import fitz  # PyMuPDF
    PDF_SUPPORT = True
except ImportError:
    try:
        import pdfminer
        from pdfminer.high_level import extract_text as pdfminer_extract
        PDF_SUPPORT = "pdfminer"
    except ImportError:
        PDF_SUPPORT = False

# ─── Optional HTTP ───────────────────────────────────────────────────────────
try:
    import urllib.request
    import urllib.parse
    import urllib.error
    HTTP_AVAILABLE = True
except ImportError:
    HTTP_AVAILABLE = False

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "books.db")
SCAN_STATUS = {"running": False, "progress": 0, "total": 0, "current": "", "done": False}

SUPPORTED_EXTENSIONS = {".epub", ".pdf", ".mobi", ".azw", ".azw3", ".djvu", ".tif", ".tiff", ".cbz", ".cbr"}
AUDIO_EXTENSIONS    = {".mp3", ".m4a", ".m4b", ".aac", ".ogg", ".flac", ".opus", ".wma", ".wav"}
ALL_EXTENSIONS      = SUPPORTED_EXTENSIONS | AUDIO_EXTENSIONS
# Files to never index even if their extension matches — cover images, metadata sidecars
IGNORE_EXTENSIONS   = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp',
                       '.txt', '.nfo', '.xml', '.opf', '.htm', '.html'}

# ─── Database ────────────────────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH, timeout=15)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop("db", None)
    if db:
        db.close()

def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    # Create tables if they don't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            primary_file_path TEXT,
            isbn TEXT,
            isbn13 TEXT,
            openlibrary_id TEXT,
            google_books_id TEXT,
            lc_call_number TEXT,
            title TEXT,
            subtitle TEXT,
            author TEXT,
            publisher TEXT,
            publish_year TEXT,
            subjects TEXT,
            description TEXT,
            cover_url TEXT,
            language TEXT,
            page_count INTEGER,
            match_status TEXT DEFAULT 'unmatched',
            match_confidence TEXT DEFAULT 'none',
            manual_override INTEGER DEFAULT 0,
            notes TEXT,
            lc_class TEXT,
            lc_number TEXT,
            lc_cutter TEXT,
            lc_year TEXT,
            lc_sort TEXT,
            is_physical INTEGER DEFAULT 0,
            date_read TEXT,
            rating INTEGER,
            date_added TEXT DEFAULT (datetime('now')),
            date_updated TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS match_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER,
            source TEXT,
            external_id TEXT,
            title TEXT,
            author TEXT,
            publish_year TEXT,
            publisher TEXT,
            isbn TEXT,
            cover_url TEXT,
            raw_json TEXT,
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS book_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id INTEGER NOT NULL,
            file_path TEXT UNIQUE NOT NULL,
            file_name TEXT,
            file_ext TEXT,
            file_size INTEGER,
            file_mtime REAL,
            file_sha1 TEXT,
            date_added TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (book_id) REFERENCES books(id)
        )
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_book_files_path ON book_files(file_path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_book_files_book ON book_files(book_id)")

    # API response cache — keyed by (source, query), TTL 30 days
    conn.execute("""
        CREATE TABLE IF NOT EXISTS api_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            query TEXT NOT NULL,
            response_json TEXT,
            cached_at TEXT DEFAULT (datetime('now')),
            UNIQUE(source, query)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_api_cache_lookup ON api_cache(source, query)")

    # Migrate: add any columns that may be missing from older DB versions
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(books)").fetchall()}
    migrations = [
        ("date_added",       "TEXT DEFAULT (datetime('now'))"),
        ("date_updated",     "TEXT DEFAULT (datetime('now'))"),
        ("lc_call_number",   "TEXT"),
        ("subtitle",         "TEXT"),
        ("language",         "TEXT"),
        ("page_count",       "INTEGER"),
        ("match_confidence", "TEXT DEFAULT 'none'"),
        ("manual_override",  "INTEGER DEFAULT 0"),
        ("notes",            "TEXT"),
        ("primary_file_path", "TEXT"),
        ("lc_class",         "TEXT"),
        ("lc_number",        "TEXT"),
        ("lc_cutter",        "TEXT"),
        ("lc_year",          "TEXT"),
        ("lc_sort",          "TEXT"),
        ("is_physical",      "INTEGER DEFAULT 0"),
        ("date_read",        "TEXT"),
        ("rating",           "INTEGER"),
    ]
    for col, col_def in migrations:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE books ADD COLUMN {col} {col_def}")
    conn.commit()
    conn.close()

# ─── ISBN Extraction ─────────────────────────────────────────────────────────

# Separator character class covering all dash/hyphen Unicode variants + space
_SEP = r'[\-\u2010\u2011\u2012\u2013\u2014\u2015 ]'  # hyphen, ‐ ‑ ‒ – — ― and space

ISBN_RE = re.compile(
    r'(?:ISBN' + _SEP + r'?(?:1[03])?' + _SEP + r'?:?\s*)'
    r'((?:97[89]' + _SEP + r'?)?(?:[0-9O]' + _SEP + r'?){9}[0-9Xx])',
    re.IGNORECASE
)
# Bare ISBN-10: publisher-number-check format without ISBN label
# Catches patterns like 0-671-42517-X or 0 671 42517 X on their own line
_BARE_ISBN10_RE = re.compile(
    r'(?<![0-9A-Za-z])'
    r'((?:0[- ]?[0-9]{1,5}[- ]?[0-9]{2,7}[- ]?[0-9]{1,6}[- ]?[0-9Xx])'
    r'|(?:[0-9]{9}[0-9Xx]))'
    r'(?![0-9A-Za-z])'
)
# Bare 13-digit 978/979 numbers (no ISBN label) common in scanned PDFs
_BARE_ISBN13_RE  = re.compile(r'(?<![\d])(97[89]\d{10})(?![\d])')          # unseparated
_BARE_ISBN13S_RE = re.compile(r'(?<![\d])(97[89][\d\u2010\u2011\u2012\u2013\u2014\u2015 -]{11,17})(?![\d])')   # separated


def clean_isbn(raw):
    return re.sub(r'[^0-9Xx]', '', raw)

def validate_isbn13(s):
    if len(s) != 13 or not s.isdigit():
        return False
    total = sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(s))
    return total % 10 == 0

def validate_isbn10(s):
    if len(s) != 10:
        return False
    try:
        weights = list(range(10, 0, -1))
        total = sum(weights[i] * (10 if c.upper() == 'X' else int(c)) for i, c in enumerate(s))
        return total % 11 == 0
    except ValueError:
        return False

def isbn10_to_13(s):
    core = s[:9]
    raw = "978" + core
    check = (10 - sum(int(c) * (1 if i % 2 == 0 else 3) for i, c in enumerate(raw)) % 10) % 10
    return raw + str(check)

def best_isbn(candidates):
    """Return (isbn10, isbn13) from a list of raw candidate strings."""
    seen13, seen10 = [], []
    for raw in candidates:
        c = clean_isbn(raw)
        if validate_isbn13(c):
            seen13.append(c)
        elif validate_isbn10(c):
            seen10.append(c)
    if seen13:
        return None, seen13[0]
    if seen10:
        return seen10[0], isbn10_to_13(seen10[0])
    return None, None

def _fix_ocr_isbn(text):
    """
    Fix common OCR misreads in ISBN strings before regex matching.
    Only applies substitutions within plausible ISBN context to avoid
    corrupting non-ISBN text.
    """
    # Pre-pass 1: fix corrupted ISBN label spellings
    # "isвn", "ISRN", "ISвN" etc. — single-char corruption of "ISBN"
    text = re.sub(r'\bIS[Bв][NnИ]\b', 'ISBN', text, flags=re.IGNORECASE)   # ISвN variants
    text = re.sub(r'\bIS[Rr][NnИ]\b', 'ISBN', text, flags=re.IGNORECASE)    # ISRN
    text = re.sub(r'\b[Ii1][Ss5][Bb8][Nn]\b', 'ISBN', text)                  # l5BN, 1SBN etc.

    # Pre-pass 2: replace non-ASCII non-alphanumeric characters that appear
    # immediately between "ISBN" and the digit string with '0'.
    # This handles OCR artifacts like ○, °, ©, •, Ø appearing as the first digit.
    # Pattern: ISBN <whitespace> <non-ASCII-non-digit> <digit-or-lookalike>
    text = re.sub(
        r'(ISBN\s*[:\-\u2010\u2011\u2012\u2013\u2014\u2015]?\s*)([^\x00-\x7F])(?=[0-9OoIil\-\u2010\u2011\u2012\u2013\u2014\u2015 ])',
        lambda m: m.group(1) + '0',
        text, flags=re.IGNORECASE
    )

    # Pre-pass 3: collapse consecutive dashes/separators within ISBN digit runs.
    # "o-8o7o--1528-8" → "o-8o7o-1528-8"  (double dash is OCR artifact for em-dash)
    # Only collapse when surrounded by digit-like characters to avoid false positives.
    _DASH_CHARS = r'[\-\u2010\u2011\u2012\u2013\u2014\u2015]'
    text = re.sub(
        r'(?<=[0-9OoIilSBZzGqQ])(' + _DASH_CHARS + r'){2,}(?=[0-9OoIilSBZzGqQ])',
        '-',
        text
    )



    def clean_digit_run(m):
        prefix = m.group(1)   # "ISBN " or "978" etc.
        digits = m.group(2)   # the digit string that may have OCR errors
        # Apply character-level substitutions that are only valid in digit positions
        fixed = digits
        fixed = fixed.replace('O', '0').replace('o', '0')  # O/o -> 0
        fixed = fixed.replace('I', '1').replace('l', '1')  # I/l -> 1 (most common)
        fixed = fixed.replace('S', '5')                     # S -> 5
        fixed = fixed.replace('B', '8')                     # B -> 8 (978 prefix)
        fixed = fixed.replace('Z', '2').replace('z', '2')  # Z/z -> 2
        fixed = fixed.replace('G', '6')                     # G -> 6
        fixed = fixed.replace('q', '9').replace('Q', '9')  # q/Q -> 9
        return prefix + fixed

    # Pattern 1: after "ISBN[-10/-13][ :]" fix the following digit run
    text = re.sub(
        r'(ISBN[\-\u2010\u2011\u2012\u2013\u2014\u2015 ]?(?:1[03])?[\-\u2010\u2011\u2012\u2013\u2014\u2015 ]?:?\s*)([0-9OoIilSBZzGqQ][0-9OoIilSBZzGqQ \-\u2010\u2011\u2012\u2013\u2014\u2015]{8,17}[0-9OoIilXx])',
        clean_digit_run,
        text, flags=re.IGNORECASE
    )

    # Pattern 2: fix 978/979 prefix region (handles "97B-...", "97Z-...", "9780..." etc.)
    text = re.sub(
        r'(97[89Bz])([0-9OoIilSBZzGqQ \-\u2010\u2011\u2012\u2013\u2014\u2015]{10,16})',
        clean_digit_run,
        text, flags=re.IGNORECASE
    )

    # Pattern 3: standalone digit runs that start with a digit-or-lookalike
    # followed by hyphens (typical isbn10 bare format: O-671-42517-X)
    text = re.sub(r'(?<!\w)O(?=[-\s]?[0-9])', '0', text)
    text = re.sub(r'(?<!\w)I(?=[-\s]?[0-9])', '1', text)
    text = re.sub(r'(?<!\w)l(?=[-\s]?[0-9])', '1', text)

    return text

def extract_isbns_from_text(text):
    # Pre-process: fix common OCR misreads in digit runs near ISBN context
    text = _fix_ocr_isbn(text)

    results = [m.group(1) for m in ISBN_RE.finditer(text)]

    # Bare ISBN-10: hyphenated publisher-number patterns on their own
    # Must pass ISBN-10 check digit validation to avoid false positives
    for m in _BARE_ISBN10_RE.finditer(text):
        raw = m.group(1)
        cleaned = re.sub(r'[^0-9Xx]', '', raw).upper()
        if len(cleaned) == 10 and cleaned not in results:
            # Validate ISBN-10 check digit
            try:
                total = sum((10 - i) * (10 if c == 'X' else int(c))
                            for i, c in enumerate(cleaned))
                if total % 11 == 0:
                    results.append(raw)
            except Exception:
                pass

    # Second pass on ISBN_RE results: if an isbn10 fails validation,
    # try alternate OCR interpretations (I/l could be 0 not 1 in publisher prefix)
    final = []
    for raw in results:
        c = re.sub(r'[^0-9Xx]', '', raw).upper()
        if len(c) == 10:
            total = sum((10-i)*(10 if ch=='X' else int(ch)) for i,ch in enumerate(c))
            if total % 11 != 0:
                # Try replacing 1s that came from I/l OCR with 0
                alt = re.sub(r'(?<![0-9])1(?=[0-9])', '0', c, count=1)
                if alt != c:
                    alt_total = sum((10-i)*(10 if ch=='X' else int(ch)) for i,ch in enumerate(alt))
                    if alt_total % 11 == 0:
                        raw = alt
        if raw not in final:
            final.append(raw)
    results = final

    # Bare 13-digit 978/979 strings (no ISBN label) — unseparated and separated
    for pattern in (_BARE_ISBN13_RE, _BARE_ISBN13S_RE):
        for m in pattern.finditer(text):
            cleaned = re.sub(r'[ \-]', '', m.group(1))
            if len(cleaned) == 13 and cleaned not in results:
                results.append(cleaned)

    return results

def extract_text_epub(path, max_chars=80000):
    """
    Extract text from an EPUB in spine reading order.
    Reads files in OPF spine order (correct reading order) rather than
    alphabetical zip order. Also extracts OPF metadata text.
    Reads up to max_chars total, prioritising first ~15 content files
    (enough to always capture the copyright/CIP page).
    """
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            names_set = set(zf.namelist())
            text_parts = []

            # ── Step 1: get OPF path from container.xml ───────────────────
            opf_path = None
            opf_dir  = ''
            try:
                container = zf.read('META-INF/container.xml').decode('utf-8', errors='replace')
                m = re.search(r'full-path="([^"]+\.opf)"', container)
                if m:
                    opf_path = m.group(1)
                    opf_dir  = opf_path.rsplit('/', 1)[0] + '/' if '/' in opf_path else ''
            except Exception:
                pass

            # ── Step 2: read OPF and extract spine order + dc: metadata ───
            spine_files = []
            if opf_path and opf_path in names_set:
                try:
                    opf_xml = zf.read(opf_path).decode('utf-8', errors='replace')
                    # Pull dc: metadata (title, creator, subject, description)
                    dc_text = ' '.join(re.findall(r'<dc:[^>]+>([^<]+)</dc:', opf_xml))
                    if dc_text.strip():
                        text_parts.append(dc_text)

                    # Build id→href map from manifest
                    id_to_href = {}
                    for m in re.finditer(r'<item\b[^>]*\bid="([^"]+)"[^>]*\bhref="([^"]+)"', opf_xml):
                        id_to_href[m.group(1)] = m.group(2)

                    # Read spine idref order
                    for m in re.finditer(r'<itemref\b[^>]*\bidref="([^"]+)"', opf_xml):
                        idref = m.group(1)
                        if idref in id_to_href:
                            href = id_to_href[idref]
                            # Resolve relative to OPF directory
                            full = opf_dir + href if not href.startswith('/') else href.lstrip('/')
                            if re.search(r'\.(xhtml|html|htm)$', full, re.I):
                                spine_files.append(full)
                except Exception:
                    pass

            # ── Step 3: fall back to alphabetical if spine empty ──────────
            if not spine_files:
                spine_files = sorted(
                    n for n in names_set if re.search(r'\.(xhtml|html|htm)$', n, re.I)
                )

            # ── Step 4: read up to 15 spine files or max_chars ───────────
            read_names = set()
            for name in spine_files[:15]:
                if name not in names_set:
                    continue
                try:
                    raw   = zf.read(name).decode('utf-8', errors='replace')
                    plain = re.sub(r'<[^>]+>', ' ', raw)
                    plain = re.sub(r'\s{2,}', ' ', plain).strip()
                    text_parts.append(plain)
                    read_names.add(name)
                    if sum(len(p) for p in text_parts) >= max_chars:
                        break
                except Exception:
                    pass

            # ── Step 5: also check non-spine manifest files ───────────────
            # Copyright pages are often marked linear="no" and excluded from
            # the spine, but we still need them for ISBN/LC extraction.
            # Prioritise files with copyright-suggestive names, then read
            # remaining manifest HTML files we haven't seen yet.
            all_html = [n for n in names_set
                        if re.search(r'\.(xhtml|html|htm)$', n, re.I)
                        and n not in read_names]

            # Sort: copyright/title/front-matter names first
            def _front_priority(name):
                base = name.lower()
                if any(k in base for k in ('copyright','copyrights','legal','rights',
                                           'title','frontmatter','front_matter',
                                           'colophon','imprint','prelim')):
                    return 0
                return 1

            for name in sorted(all_html, key=_front_priority)[:10]:
                if sum(len(p) for p in text_parts) >= max_chars:
                    break
                try:
                    raw   = zf.read(name).decode('utf-8', errors='replace')
                    plain = re.sub(r'<[^>]+>', ' ', raw)
                    plain = re.sub(r'\s{2,}', ' ', plain).strip()
                    # Only include if it looks like front matter (has ISBN/LC/copyright keywords)
                    if re.search(r'isbn|copyright|cataloging|lcc\s*:|call.?no|97[89]\d{10}|\d{9}[\dXx]', plain, re.I):
                        text_parts.append(plain)
                except Exception:
                    pass

            return '\n'.join(text_parts)
    except Exception:
        return ''

def extract_text_pdf(path, max_pages=10):
    """Extract text from first N pages of PDF."""
    if not PDF_SUPPORT:
        return ''
    try:
        if PDF_SUPPORT is True:  # fitz/PyMuPDF
            doc = fitz.open(path)
            pages = min(max_pages, len(doc))
            return ' '.join(doc[i].get_text() for i in range(pages))
        elif PDF_SUPPORT == "pdfminer":
            return pdfminer_extract(path, maxpages=max_pages) or ''
    except Exception:
        return ''

# Valid LC class prefixes (single and double letter)
_LC_CLASSES = {
    'A','B','C','D','E','F','G','H','J','K','L','M','N','P','Q','R','S','T','U','V','Z',
    'BF','BL','BQ','BR','BS','BT','BV','BX','CB','CC','CD','CR','CS','CT',
    'DA','DC','DD','DE','DF','DG','DH','DJ','DK','DL','DP','DQ','DR','DS','DT','DU','DX',
    'GE','GF','GN','GR','GT','GV','HA','HB','HC','HD','HE','HF','HG','HJ','HM','HN',
    'HQ','HS','HT','HV','HX','JA','JC','JF','JK','JL','JN','JP','JQ','JS','JV','JX','JZ',
    'KD','KE','KF','KG','KH','KJ','KK','KL','KM','KN','KP','KQ','KR','KS','KT','KU','KV','KW','KZ',
    'LA','LB','LC','LD','LE','LF','LG','LH','LJ','LT','ML','MT',
    'NA','NB','NC','ND','NE','NK','NX','PA','PB','PC','PD','PE','PF','PG','PH','PJ','PK',
    'PL','PM','PN','PQ','PR','PS','PT','PZ','QA','QB','QC','QD','QE','QH','QK','QL','QM','QP','QR',
    'RA','RB','RC','RD','RE','RF','RG','RJ','RK','RL','RM','RS','RT','RV','RX','RZ',
    'SB','SD','SF','SH','SK','TA','TC','TD','TE','TF','TG','TH','TJ','TK','TL','TN','TP','TR','TS','TT','TX',
    'UA','UB','UC','UD','UE','UF','UG','UH','VA','VB','VC','VD','VE','VF','VG','VK','VM','ZA'
}

# Pattern: 1-3 uppercase letters, digits, at least one Cutter (.Letter+digits)
_LC_PATTERN = re.compile(
    r'\b([A-Z]{1,3}\d{1,5}(?:\.\d+)?'         # class letters + digits
    r'(?:\s*\.[A-Z]\d*[A-Z]?\d*){1,2}'        # 1-2 dot-Cutter numbers
    r'(?:\s+[A-Z]\d+)?'                         # optional space-cutter (e.g. A3, B7)
    r'(?:\s+\d{4}[a-z]{0,3})?)'                # optional year + suffix (eb, b, x, etc.)
)

# Explicit label pattern - most reliable
_LC_LABELLED = re.compile(
    r'(?:lcc|LC|Library\s+of\s+Congress|Call\s+[Nn]o\.?|Classification)\s*[:\s]\s*'
    r'([A-Z]{1,3}\d{1,5}(?:\.\d+)?(?:\.[A-Z]\d+)+(?:\s+[A-Z]\d+)?(?:\s+\d{4}[a-z]{0,3})?)',
    re.IGNORECASE
)

def extract_lc_from_text(text):
    if not text:
        return ''
    # Try labelled first — high confidence
    m = _LC_LABELLED.search(text)
    if m:
        return m.group(1).strip()

    # Pre-pass: find LC numbers inside brackets [CLASS.CUTTER YEAR] in CIP lines.
    # e.g. "PZ4.I714Wo 1978 [PS3559.R68]" — the bracketed one is the preferred shelf number.
    bracketed = re.findall(
        r'\[([A-Z]{1,3}\d{1,5}(?:\.\d+)?(?:\.[A-Z]\d*)+(?:\s+[A-Z]\d+)?(?:\s+\d{4}[a-z]{0,3})?)\]',
        text
    )
    for candidate in bracketed:
        prefix_m = re.match(r'[A-Z]+', candidate)
        if prefix_m and prefix_m.group() in _LC_CLASSES:
            return candidate.strip()

    # Unlabelled: search near copyright-page anchor words
    anchor = re.search(
        r'(?:isbn|copyright|published by|all rights reserved|cataloging.in.publication|lcc\s*:).{0,1200}',
        text, re.IGNORECASE | re.DOTALL
    )
    # Primary: search within anchor window
    search_text = anchor.group(0) if anchor else text[:5000]
    for m in _LC_PATTERN.finditer(search_text):
        candidate = m.group(1).strip()
        prefix_m = re.match(r'[A-Z]+', candidate)
        if prefix_m and prefix_m.group() in _LC_CLASSES:
            return candidate
    # Fallback: scan full first 5000 chars (catches call numbers far from ISBN)
    if anchor:
        for m in _LC_PATTERN.finditer(text[:5000]):
            candidate = m.group(1).strip()
            prefix_m = re.match(r'[A-Z]+', candidate)
            if prefix_m and prefix_m.group() in _LC_CLASSES:
                return candidate
    return ''


def extract_text_mobi(path, max_chars=80000):
    """
    Extract text from MOBI/AZW/AZW3 files.
    AZW3 is ZIP-based (like EPUB). MOBI is PalmDB binary with embedded HTML.
    """
    ext = Path(path).suffix.lower()

    # AZW3 files are ZIP/EPUB containers — try that first
    if ext in ('.azw3', '.azw'):
        try:
            result = extract_text_epub(path)
            if result.strip():
                return result
        except Exception:
            pass

    try:
        fsize = os.path.getsize(path)
        with open(path, 'rb') as f:
            # Read up to 1MB — covers copyright pages even in large MOBI files
            # (copyright at 1% of a 5MB book = ~50KB; needs headroom for larger files)
            data = f.read(min(1024 * 1024, fsize))

        # Decode preserving all bytes
        raw = data.decode('latin-1', errors='replace')

        # Strategy 1: find all HTML-like content (MOBI stores text as HTML internally)
        # Strip null bytes and control chars first so regex works cleanly
        clean = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', ' ', raw)
        
        # Find everything between HTML tags (MOBI uses <p>, <div>, etc.)
        html_text = re.sub(r'<[^>]{0,200}>', ' ', clean)
        
        # Extract readable runs — must be at least 20 printable chars
        # IMPORTANT: in large MOBIs, the copyright page appears near the END of the
        # binary buffer (after record headers). One giant space-run can push the
        # ISBN past the max_chars cutoff.
        # Solution: always include the TAIL of the buffer as well as the head.
        readable_runs = re.findall(r'[ -~\n\r\t]{20,}', html_text)
        if readable_runs:
            # Head: first 40KB worth
            head = '\n'.join(readable_runs)[:40000]
            # Tail: last 40KB of the buffer (where copyright page tends to sit)
            tail = html_text[-40000:]
            combined = head + '\n' + tail
            if re.search(r'[A-Za-z]{3,}', combined):
                return combined[:max_chars]

        # Strategy 2: raw byte scan — always include both head and tail
        raw_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', ' ', raw)
        head_chunks = re.findall(r'[A-Za-z0-9 .,:;\'\"\-/\(\)]{15,}', raw_text[:40000])
        tail_chunks = re.findall(r'[A-Za-z0-9 .,:;\'\"\-/\(\)]{15,}', raw_text[-40000:])
        return ('\n'.join(head_chunks) + '\n' + '\n'.join(tail_chunks))[:max_chars]

    except Exception:
        return ''

def extract_isbn_from_file(path):
    ext = Path(path).suffix.lower()
    text = ''
    if ext == '.epub':
        text = extract_text_epub(path)
    elif ext == '.pdf':
        text = extract_text_pdf(path)
    elif ext in ('.mobi', '.azw', '.azw3'):
        text = extract_text_mobi(path)

    candidates = extract_isbns_from_text(text)
    isbn10, isbn13 = best_isbn(candidates)
    lc = extract_lc_from_text(text)
    return isbn10, isbn13, lc

# ─── SHA1 Hashing ────────────────────────────────────────────────────────────

def sha1_file(path, chunk_size=1024 * 1024):
    """SHA1 of file contents. Used to detect renames/moves."""
    import hashlib
    h = hashlib.sha1()
    try:
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

# ─── LC Call Number Parser ────────────────────────────────────────────────────

# LC call number structure: CLASS  NUMBER[.DECIMAL]  CUTTER[CUTTER2]  [YEAR]
# e.g. PS3552.A45 2005  →  class=PS  number=3552  cutter=A45  year=2005
# e.g. DS135.P62W37     →  class=DS  number=135   cutter=P62W37
_LC_PARSE_RE = re.compile(
    r'^([A-Z]{1,3})'           # class letters
    r'(\d{1,5}(?:\.\d+)?)'     # number (with optional decimal)
    r'(?:\s*\.([A-Z]\d*\w*))?' # primary cutter (.A45 or .P62W37)
    r'(?:\s+(\d{4}))?',        # optional year
    re.IGNORECASE
)

def parse_lc(lc_raw):
    """
    Parse a raw LC call number into components and produce a sortable key.
    Returns dict with keys: lc_class, lc_number, lc_cutter, lc_year, lc_sort
    All values are strings or empty string if not found.

    Sort key format: CLASS_PADDED|NUMBER_PADDED|CUTTER_PADDED|YEAR
    This makes alphabetical sort on lc_sort equivalent to shelf order.
    """
    if not lc_raw:
        return {}
    m = _LC_PARSE_RE.match(lc_raw.strip())
    if not m:
        return {}

    lc_class  = (m.group(1) or '').upper()
    lc_number = m.group(2) or ''
    lc_cutter = (m.group(3) or '').upper()
    lc_year   = m.group(4) or ''

    # Build sortable key
    # Class: pad to 3 chars with spaces so AA < AAA, B < BA etc.
    class_sort = lc_class.ljust(3)

    # Number: split on decimal, zero-pad integer part to 5 digits
    if '.' in lc_number:
        int_part, dec_part = lc_number.split('.', 1)
        num_sort = int_part.zfill(5) + '.' + dec_part
    else:
        num_sort = lc_number.zfill(5)

    # Cutter: pad letter to 1, pad digits to 4
    if lc_cutter:
        cut_letter = lc_cutter[0]
        cut_digits = re.sub(r'[^0-9]', '', lc_cutter[1:]).zfill(4)
        cut_sort = cut_letter + cut_digits
    else:
        cut_sort = ''

    lc_sort = f"{class_sort}|{num_sort}|{cut_sort}|{lc_year}"

    return {
        'lc_class':  lc_class,
        'lc_number': lc_number,
        'lc_cutter': lc_cutter,
        'lc_year':   lc_year,
        'lc_sort':   lc_sort,
    }


# ─── Library of Congress SRU Lookup ──────────────────────────────────────────

def query_loc_for_lc_number(isbn):
    """
    Query the Library of Congress SRU endpoint by ISBN to get the LC call number.
    Free, no key required. Returns call number string or None.
    """
    if not isbn:
        return None
    cached = _cache_get('loc_sru', isbn)
    if cached is not None:
        return cached if cached else None
    url = (
        'https://lccn.loc.gov/sru?version=1.1&operation=searchRetrieve'
        '&recordSchema=marcxml&maximumRecords=3'
        f'&query=bath.isbn%3D{isbn}'
    )
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={'User-Agent': 'BookMeta/1.0'})
        with urllib.request.urlopen(req, timeout=6) as r:
            xml = r.read().decode('utf-8', errors='replace')
        m_a = re.search(r'tag=["\']050["\'][^>]*>.*?code=["\']a["\'][^>]*>([^<]+)', xml, re.DOTALL)
        m_b = re.search(r'tag=["\']050["\'][^>]*>.*?code=["\']b["\'][^>]*>([^<]+)', xml, re.DOTALL)
        if m_a:
            call = m_a.group(1).strip()
            if m_b:
                call = call + ' ' + m_b.group(1).strip()
            prefix = re.match(r'[A-Z]+', call)
            if prefix and prefix.group() in _LC_CLASSES:
                _cache_set('loc_sru', isbn, call)
                return call.strip()
        # Cache negative result so we don't retry
        _cache_set('loc_sru', isbn, '')
    except Exception:
        pass
    return None


def query_oclc_classify_for_lc(isbn):
    """
    Query OCLC Classify API by ISBN to get LC call number.
    Free, no key required. Often has LC data when LOC SRU misses it.
    Returns call number string or None.
    """
    if not isbn:
        return None
    cached = _cache_get('oclc_classify', isbn)
    if cached is not None:
        return cached if cached else None
    url = f'http://classify.oclc.org/classify2/Classify?isbn={isbn}&summary=true'
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={'User-Agent': 'BookMeta/1.0'})
        with urllib.request.urlopen(req, timeout=6) as r:
            xml = r.read().decode('utf-8', errors='replace')
        m = re.search(r'<lcc>\s*<mostPopular[^>]+nsfa=["\']([^"\']+)["\']', xml)
        if not m:
            m = re.search(r'<lcc>\s*<mostPopular[^>]+sfa=["\']([^"\']+)["\']', xml)
        if m:
            call = m.group(1).strip()
            prefix = re.match(r'[A-Z]+', call)
            if prefix and prefix.group() in _LC_CLASSES:
                _cache_set('oclc_classify', isbn, call)
                return call
        _cache_set('oclc_classify', isbn, '')
    except Exception:
        pass
    return None

# ─── Metadata Lookup ─────────────────────────────────────────────────────────

def http_get(url, timeout=6):
    if not HTTP_AVAILABLE:
        return None
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'BookMeta/1.0'})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception:
        return None

def _cache_get(source, query):
    """Return cached response data if present and not expired (30 days)."""
    for _ in range(3):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            conn.execute("PRAGMA journal_mode=WAL")
            row = conn.execute(
                "SELECT response_json FROM api_cache WHERE source=? AND query=? "
                "AND cached_at > datetime('now', '-30 days')",
                (source, query)
            ).fetchone()
            conn.close()
            if row is not None:
                return json.loads(row[0]) if row[0] else ''
            return None
        except Exception:
            pass
    return None

def _cache_set(source, query, data):
    """Store API response in cache, retrying up to 3 times on lock."""
    value = json.dumps(data) if (data is not None and data != '') else ''
    for _ in range(3):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "INSERT OR REPLACE INTO api_cache (source, query, response_json, cached_at) "
                "VALUES (?, ?, ?, datetime('now'))",
                (source, query, value)
            )
            conn.commit()
            conn.close()
            return
        except Exception:
            import time; time.sleep(0.5)

def cached_http_get(source, query, url):
    """http_get with transparent caching by (source, query) key."""
    cached = _cache_get(source, query)
    if cached is not None:
        return cached
    data = http_get(url)
    if data is not None:
        _cache_set(source, query, data)
    return data

# Optional Google Books API key — stored in a file next to the DB for persistence
_GOOGLE_API_KEY = None
def _get_google_api_key():
    global _GOOGLE_API_KEY
    if _GOOGLE_API_KEY:
        return _GOOGLE_API_KEY
    key_path = os.path.join(os.path.dirname(DB_PATH), 'google_api_key.txt')
    if os.path.isfile(key_path):
        with open(key_path) as f:
            _GOOGLE_API_KEY = f.read().strip()
    return _GOOGLE_API_KEY

def _google_url(base):
    key = _get_google_api_key()
    return base + (f'&key={key}' if key else '')

def _clean_subjects(raw_subjects, max_subjects=8):
    """
    Clean a subjects list from any source into a tidy comma-separated string.
    Filters out: non-English entries, call-number-like strings, internal codes,
    overly long provenance notes, duplicates, and single-char entries.
    Accepts: list of strings, list of dicts with 'name' key, or a raw string.
    """
    if not raw_subjects:
        return ''
    # Normalize to list of strings
    if isinstance(raw_subjects, str):
        items = [s.strip() for s in raw_subjects.split(',')]
    elif isinstance(raw_subjects, list):
        items = []
        for s in raw_subjects:
            if isinstance(s, dict):
                items.append(s.get('name', ''))
            else:
                items.append(str(s))
    else:
        return ''

    # Common non-English subject words to skip (LC/OL frequently mix in foreign terms)
    _foreign = {
        'histoire', 'kunst', 'geschichte', 'kunst', 'philosophie', 'litterature',
        'literatur', 'wissenschaft', 'recht', 'politik', 'wirtschaft', 'sprache',
        'musique', 'droit', 'societe', 'gesellschaft', 'arte', 'storia', 'diritto',
        'economia', 'filosofia', 'letteratura', 'matematica', 'fisica', 'chimica',
        'biologia', 'historia', 'derecho', 'politica', 'educacion', 'sociologia',
        'psicologia', 'antropologia', 'geografia', 'lingüística', 'linguistica',
    }

    cleaned = []
    seen = set()
    for item in items:
        item = item.strip()
        if not item or len(item) < 3:
            continue
        # Skip call-number-like strings
        if re.match(r'^[A-Z]{1,3}\d+', item):
            continue
        # Skip short all-caps codes and date codes like "CHR 1991"
        if re.match(r'^[A-Z]{2,4}\s+\d{4}$', item):
            continue
        # Skip provenance/owner notes
        if re.search(r'\bformer owner\b|\bCollection copy\b|^PRO\b', item, re.I):
            continue
        # Skip non-ASCII-majority strings
        ascii_ratio = sum(1 for c in item if ord(c) < 128) / max(len(item), 1)
        if ascii_ratio < 0.8:
            continue
        # Skip overly long entries (> 60 chars)
        if len(item) > 60:
            continue
        # Skip known foreign-language subject words (including multi-word phrases starting with them)
        first_word = item.split()[0].lower().rstrip("'")
        if first_word in _foreign or item.lower() in _foreign:
            continue
        # Normalize
        item = item.strip('.,;: ')
        lower = item.lower()
        if lower in seen:
            continue
        seen.add(lower)
        cleaned.append(item)
        if len(cleaned) >= max_subjects:
            break

    return ', '.join(cleaned)

def query_openlibrary_isbn(isbn):
    data = cached_http_get('ol_isbn', isbn,
        f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data")
    if not data:
        return []
    key = f"ISBN:{isbn}"
    if key not in data:
        return []
    book = data[key]
    authors = ', '.join(a.get('name', '') for a in book.get('authors', []))
    subjects = _clean_subjects(book.get('subjects', []))
    cover = book.get('cover', {}).get('medium', '')
    ol_id = book.get('key', '').replace('/works/', '')
    pub_date = book.get('publish_date', '')
    year = re.search(r'\d{4}', pub_date)
    publisher = ', '.join(p.get('name', '') for p in book.get('publishers', []))
    return [{
        'source': 'OpenLibrary',
        'external_id': ol_id,
        'title': book.get('title', ''),
        'subtitle': book.get('subtitle', ''),
        'author': authors,
        'publish_year': year.group() if year else pub_date,
        'publisher': publisher,
        'isbn': isbn,
        'cover_url': cover,
        'subjects': subjects,
        'description': book.get('notes', {}).get('value', '') if isinstance(book.get('notes'), dict) else str(book.get('notes', '')),
        'lc_call_number': '',
        'language': '',
        'page_count': book.get('number_of_pages', 0),
        'raw_json': json.dumps(book)
    }]

def query_google_books_isbn(isbn):
    data = cached_http_get('gb_isbn', isbn,
        _google_url(f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"))
    return _parse_google(data)

def query_google_books_title(title, author=''):
    q = urllib.parse.quote(f"intitle:{title}" + (f"+inauthor:{author}" if author else ''))
    cache_key = f"intitle:{title}" + (f"+inauthor:{author}" if author else '')
    data = cached_http_get('gb_title', cache_key,
        _google_url(f"https://www.googleapis.com/books/v1/volumes?q={q}&maxResults=5"))
    return _parse_google(data)

def query_openlibrary_title(title):
    """Search OpenLibrary by title. Returns list of candidates."""
    q = urllib.parse.quote(title)
    data = cached_http_get('ol_title', title,
        f"https://openlibrary.org/search.json?title={q}&limit=5&fields=key,title,author_name,first_publish_year,publisher,isbn,subject,lcc")
    if not data or 'docs' not in data:
        return []
    results = []
    for doc in data['docs'][:5]:
        isbns = doc.get('isbn', [])
        isbn13 = next((i for i in isbns if len(i) == 13), '')
        isbn10 = next((i for i in isbns if len(i) == 10), '')
        isbn = isbn13 or isbn10
        authors = ', '.join(doc.get('author_name', []))
        publishers = doc.get('publisher', [])
        lcc_list = doc.get('lcc', [])
        lcc = lcc_list[0] if lcc_list else ''
        ol_key = doc.get('key', '')
        results.append({
            'source': 'OpenLibrary',
            'external_id': ol_key,
            'title': doc.get('title', ''),
            'subtitle': '',
            'author': authors,
            'publish_year': str(doc.get('first_publish_year', '')),
            'publisher': publishers[0] if publishers else '',
            'isbn': isbn,
            'cover_url': f"https://covers.openlibrary.org/b/isbn/{isbn}-M.jpg" if isbn else '',
            'subjects': _clean_subjects(doc.get('subject', [])),
            'description': '',
            'lc_call_number': lcc,
            'language': '',
            'page_count': 0,
            'raw_json': json.dumps(doc)
        })
    return results

def _parse_google(data):
    if not data or 'items' not in data:
        return []
    results = []
    for item in data['items'][:5]:
        info = item.get('volumeInfo', {})
        idents = {i['type']: i['identifier'] for i in info.get('industryIdentifiers', [])}
        isbn = idents.get('ISBN_13') or idents.get('ISBN_10', '')
        lc = info.get('categories', [])
        authors = ', '.join(info.get('authors', []))
        pub_date = info.get('publishedDate', '')
        year = re.search(r'\d{4}', pub_date)
        results.append({
            'source': 'GoogleBooks',
            'external_id': item.get('id', ''),
            'title': info.get('title', ''),
            'subtitle': info.get('subtitle', ''),
            'author': authors,
            'publish_year': year.group() if year else pub_date,
            'publisher': info.get('publisher', ''),
            'isbn': isbn,
            'cover_url': info.get('imageLinks', {}).get('thumbnail', ''),
            'subjects': _clean_subjects(info.get('categories', [])),
            'description': info.get('description', '')[:500],
            'lc_call_number': '',
            'language': info.get('language', ''),
            'page_count': info.get('pageCount', 0),
            'raw_json': json.dumps(info)
        })
    return results

def lookup_metadata(isbn13, isbn10, filename):
    """Query all sources and return list of candidate matches."""
    candidates = []
    if isbn13:
        candidates += query_openlibrary_isbn(isbn13)
        candidates += query_google_books_isbn(isbn13)
    elif isbn10:
        candidates += query_openlibrary_isbn(isbn10)
        candidates += query_google_books_isbn(isbn10)
    
    if not candidates:
        # Fallback: title guess from filename
        stem = Path(filename).stem
        # Remove common junk
        title_guess = re.sub(r'[\(\[\{].*?[\)\]\}]', '', stem)
        title_guess = re.sub(r'[-_\.]+', ' ', title_guess).strip()
        candidates += query_google_books_title(title_guess)
        candidates += query_openlibrary_title(title_guess)
    
    return candidates

# ─── Scanner ─────────────────────────────────────────────────────────────────

def scan_library(library_path, rescan=False):
    """
    Two-phase scan:
    Phase 0 - Cleanup: find DB records whose file path no longer exists.
              If the same SHA1 is found elsewhere in the library → update path (moved).
              If not found → delete the book_files row and orphaned books record.
    Phase 1 - Fast: walk filesystem, register every file in DB, extract ISBNs.
              Books appear in UI immediately. No API calls.
    Phase 2 - Slow: for each book with an ISBN that needs lookup, hit APIs.
              Can be interrupted; progress is saved after each book.
    """
    global SCAN_STATUS
    SCAN_STATUS = {"running": True, "progress": 0, "total": 0, "current": "",
                   "phase": "indexing", "done": False}

    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    SCAN_STATUS["phase"] = "cleanup"
    SCAN_STATUS["current"] = "Checking for moved or deleted files…"

    # Walk disk ONCE to get all known paths.
    # Store both raw paths (for DB insertion) and normalized paths (for comparison)
    disk_paths_raw = []  # raw paths as os.path.join produces them
    disk_paths_norm = set()  # normalized for comparison against DB records
    for root, dirs, files in os.walk(library_path):
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        for f in files:
            ext = Path(f).suffix.lower()
            if ext in ALL_EXTENSIONS and ext not in IGNORE_EXTENSIONS:
                raw = os.path.join(root, f)
                disk_paths_raw.append(raw)
                disk_paths_norm.add(os.path.normcase(raw))

    # Find DB records whose path isn't on disk
    # Normalize both sides so slash direction and casing don't cause false mismatches
    missing_files = []
    for row in conn.execute("""
        SELECT bf.id, bf.book_id, bf.file_path, bf.file_name, bf.file_sha1
        FROM book_files bf
        JOIN books b ON b.id = bf.book_id
        WHERE b.is_physical = 0
    """).fetchall():
        if os.path.normcase(row['file_path'] or '') not in disk_paths_norm:
            missing_files.append(dict(row))

    # Phase 1 uses raw paths for DB lookups
    disk_paths = disk_paths_raw

    if missing_files:
        # Only compute SHA1s if at least one missing file has a known SHA1
        # (needed to detect moves vs deletions)
        missing_with_sha1 = [mf for mf in missing_files if mf.get('file_sha1')]
        disk_sha1_index = {}
        if missing_with_sha1:
            for full in disk_paths:
                h = sha1_file(full)
                if h:
                    disk_sha1_index[h] = full

        for mf in missing_files:
            old_sha1 = mf.get('file_sha1')
            new_path = disk_sha1_index.get(old_sha1) if old_sha1 else None

            if new_path:
                # File moved — update path
                p = Path(new_path)
                stat = os.stat(new_path)
                conn.execute(
                    "UPDATE book_files SET file_path=?, file_name=?, file_ext=?, "
                    "file_size=?, file_mtime=? WHERE id=?",
                    (new_path, p.name, p.suffix.lower(), stat.st_size, stat.st_mtime, mf['id'])
                )
                conn.execute(
                    "UPDATE books SET primary_file_path=?, date_updated=datetime('now') "
                    "WHERE id=? AND (primary_file_path=? OR primary_file_path IS NULL)",
                    (new_path, mf['book_id'], mf['file_path'])
                )
            else:
                # File gone — delete book_files row
                conn.execute("DELETE FROM book_files WHERE id=?", (mf['id'],))
                # Delete the books record if it has no remaining files
                remaining = conn.execute(
                    "SELECT COUNT(*) FROM book_files WHERE book_id=?", (mf['book_id'],)
                ).fetchone()[0]
                if remaining == 0:
                    conn.execute("DELETE FROM match_candidates WHERE book_id=?", (mf['book_id'],))
                    conn.execute("DELETE FROM books WHERE id=?", (mf['book_id'],))

        conn.commit()

    # ── Phase 1: filesystem walk + ISBN extraction (fast, no network) ──────────
    # Reuse disk_paths already collected in Phase 0 — avoid walking network drive twice
    all_files = sorted(disk_paths)

    SCAN_STATUS["total"] = len(all_files)
    SCAN_STATUS["phase"] = "indexing"

    needs_lookup = []  # (book_id, isbn13, isbn10, filename)

    for i, fpath in enumerate(all_files):
        SCAN_STATUS["progress"] = i
        SCAN_STATUS["current"] = os.path.basename(fpath)

        stat = os.stat(fpath)
        p    = Path(fpath)

        # ── Check if this file path is already in book_files ──────────────
        existing_file = conn.execute(
            "SELECT id, book_id, file_size, file_mtime, file_sha1 FROM book_files WHERE file_path=?",
            (fpath,)
        ).fetchone()
        # Fallback: try case-insensitive match in case path casing changed
        if not existing_file:
            existing_file = conn.execute(
                "SELECT id, book_id, file_size, file_mtime, file_sha1 FROM book_files WHERE LOWER(file_path)=LOWER(?)",
                (fpath,)
            ).fetchone()
            if existing_file:
                # Update stored path to current casing
                conn.execute("UPDATE book_files SET file_path=? WHERE id=?", (fpath, existing_file['id']))

        if existing_file:
            # File already registered — skip if unchanged
            if (existing_file['file_size'] == stat.st_size and
                    existing_file['file_mtime'] and
                    abs(float(existing_file['file_mtime']) - stat.st_mtime) < 2):
                book_row = conn.execute(
                    "SELECT id, match_status, isbn13, manual_override FROM books WHERE id=?",
                    (existing_file['book_id'],)
                ).fetchone()
                # Add to Phase 2 queue if unmatched and has an ISBN (rescan retries these)
                if book_row and book_row['isbn13'] and not book_row['manual_override']:
                    if book_row['match_status'] == 'unmatched' or (rescan and book_row['match_status'] not in ('confirmed', 'auto_matched')):
                        needs_lookup.append((book_row['id'], book_row['isbn13'], None, p.name))
                continue
            # mtime/size changed — check sha1
            new_sha1 = sha1_file(fpath)
            if new_sha1 and existing_file['file_sha1'] and new_sha1 == existing_file['file_sha1']:
                conn.execute(
                    "UPDATE book_files SET file_size=?, file_mtime=? WHERE id=?",
                    (stat.st_size, stat.st_mtime, existing_file['id'])
                )
                continue
            # Content actually changed — re-extract ISBN/LC, update file record
            isbn10, isbn13, lc_found = extract_isbn_from_file(fpath)
            lc_parts = parse_lc(lc_found)
            conn.execute(
                "UPDATE book_files SET file_size=?, file_mtime=?, file_sha1=? WHERE id=?",
                (stat.st_size, stat.st_mtime, new_sha1, existing_file['id'])
            )
            book_id = existing_file['book_id']
            # Update LC on the book record if we found one
            if lc_found:
                conn.execute("""
                    UPDATE books SET isbn=COALESCE(NULLIF(isbn,''),?),
                    isbn13=COALESCE(NULLIF(isbn13,''),?),
                    lc_call_number=COALESCE(NULLIF(lc_call_number,''), ?),
                    lc_class=COALESCE(NULLIF(lc_class,''), ?),
                    lc_number=COALESCE(NULLIF(lc_number,''), ?),
                    lc_cutter=COALESCE(NULLIF(lc_cutter,''), ?),
                    lc_year=COALESCE(NULLIF(lc_year,''), ?),
                    lc_sort=COALESCE(NULLIF(lc_sort,''), ?),
                    date_updated=datetime('now') WHERE id=?
                """, (isbn10, isbn13, lc_found,
                      lc_parts.get('lc_class'), lc_parts.get('lc_number'),
                      lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
                      lc_parts.get('lc_sort'), book_id))
            if isbn13:
                needs_lookup.append((book_id, isbn13, isbn10, p.name))
            continue

        # ── New file path — extract ISBN/LC ───────────────────────────────
        new_sha1 = sha1_file(fpath)

        # Check if same content exists under a different path (rename/move)
        if new_sha1:
            moved_file = conn.execute(
                "SELECT id, book_id FROM book_files WHERE file_sha1=? AND file_path!=?",
                (new_sha1, fpath)
            ).fetchone()
            if moved_file:
                conn.execute(
                    "UPDATE book_files SET file_path=?, file_name=?, file_ext=?, "
                    "file_size=?, file_mtime=? WHERE id=?",
                    (fpath, p.name, p.suffix.lower(), stat.st_size, stat.st_mtime, moved_file['id'])
                )
                if i % 50 == 0:
                    conn.commit()
                continue

        isbn10, isbn13, lc_found = extract_isbn_from_file(fpath)
        lc_parts = parse_lc(lc_found)

        # ── Find or create the book record ────────────────────────────────
        book_id = None
        if isbn13:
            # Does a book with this ISBN already exist? (duplicate format)
            existing_book = conn.execute(
                "SELECT id, match_status FROM books WHERE isbn13=? AND is_physical=0",
                (isbn13,)
            ).fetchone()
            if existing_book:
                book_id = existing_book['id']
                # Update LC on existing book if we have it and it doesn't yet
                if lc_found:
                    conn.execute("""
                        UPDATE books SET
                        lc_call_number=COALESCE(NULLIF(lc_call_number,''), ?),
                        lc_class=COALESCE(NULLIF(lc_class,''), ?),
                        lc_number=COALESCE(NULLIF(lc_number,''), ?),
                        lc_cutter=COALESCE(NULLIF(lc_cutter,''), ?),
                        lc_year=COALESCE(NULLIF(lc_year,''), ?),
                        lc_sort=COALESCE(NULLIF(lc_sort,''), ?)
                        WHERE id=?
                    """, (lc_found,
                          lc_parts.get('lc_class'), lc_parts.get('lc_number'),
                          lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
                          lc_parts.get('lc_sort'), book_id))

        if book_id is None:
            # Create new book record
            cur = conn.execute("""
                INSERT INTO books
                (isbn, isbn13, lc_call_number, lc_class, lc_number, lc_cutter, lc_year, lc_sort)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (isbn10, isbn13,
                  lc_found or None,
                  lc_parts.get('lc_class'), lc_parts.get('lc_number'),
                  lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
                  lc_parts.get('lc_sort')))
            book_id = cur.lastrowid

        # ── Register this file in book_files ─────────────────────────────
        conn.execute("""
            INSERT OR IGNORE INTO book_files
            (book_id, file_path, file_name, file_ext, file_size, file_mtime, file_sha1)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (book_id, fpath, p.name, p.suffix.lower(), stat.st_size, stat.st_mtime, new_sha1))

        # Update primary_file_path if not set (prefer epub over pdf, etc.)
        conn.execute("""
            UPDATE books SET primary_file_path=?
            WHERE id=? AND (primary_file_path IS NULL OR primary_file_path='')
        """, (fpath, book_id))

        if isbn13:
            needs_lookup.append((book_id, isbn13, isbn10, p.name))

        if i % 50 == 0:
            conn.commit()

    conn.commit()

    # ── Phase 2: API lookups (network, one book at a time) ────────────────────
    SCAN_STATUS["phase"] = "enriching"
    SCAN_STATUS["progress"] = 0
    SCAN_STATUS["total"] = len(needs_lookup)

    for i, (book_id, isbn13, isbn10, fname) in enumerate(needs_lookup):
        SCAN_STATUS["progress"] = i
        SCAN_STATUS["current"] = fname

        # Skip if already manually overridden or matched
        row = conn.execute("SELECT match_status, manual_override FROM books WHERE id=?", (book_id,)).fetchone()
        if row and (row['manual_override'] or row['match_status'] in ('confirmed', 'auto_matched')):
            continue

        # ── Do all network calls BEFORE touching the DB connection ──
        try:
            candidates = lookup_metadata(isbn13, isbn10, fname)
        except Exception:
            candidates = []

        if not candidates:
            continue

        lc_from_api = None
        best = candidates[0]
        try:
            raw = json.loads(best.get('raw_json') or '{}')
            if best['source'] == 'OpenLibrary':
                lc_list = (raw.get('classifications') or {}).get('lc_classifications') or []
                if lc_list:
                    lc_from_api = lc_list[0].strip()
        except Exception:
            pass
        if not lc_from_api:
            try:
                lc_from_api = query_loc_for_lc_number(isbn13 or isbn10)
            except Exception:
                pass
        if not lc_from_api:
            try:
                lc_from_api = query_oclc_classify_for_lc(isbn13 or isbn10)
            except Exception:
                pass

        lc_parts = parse_lc(lc_from_api) if lc_from_api else {}

        # ── Now write to DB in a short burst ──
        conn.execute("DELETE FROM match_candidates WHERE book_id=?", (book_id,))
        for c in candidates[:10]:
            conn.execute("""
                INSERT INTO match_candidates
                (book_id, source, external_id, title, author, publish_year,
                 publisher, isbn, cover_url, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (book_id, c['source'], c['external_id'], c['title'], c['author'],
                  c['publish_year'], c['publisher'], c['isbn'], c['cover_url'], c['raw_json']))
        conn.execute("""
            UPDATE books SET
                title=COALESCE(NULLIF(title,''), ?),
                author=COALESCE(NULLIF(author,''), ?),
                publish_year=COALESCE(NULLIF(publish_year,''), ?),
                publisher=COALESCE(NULLIF(publisher,''), ?),
                cover_url=COALESCE(NULLIF(cover_url,''), ?),
                subjects=COALESCE(NULLIF(subjects,''), ?),
                description=COALESCE(NULLIF(description,''), ?),
                openlibrary_id=?, google_books_id=?,
                lc_call_number=COALESCE(NULLIF(lc_call_number,''), ?),
                lc_class=COALESCE(NULLIF(lc_class,''), ?),
                lc_number=COALESCE(NULLIF(lc_number,''), ?),
                lc_cutter=COALESCE(NULLIF(lc_cutter,''), ?),
                lc_year=COALESCE(NULLIF(lc_year,''), ?),
                lc_sort=COALESCE(NULLIF(lc_sort,''), ?),
                match_status='auto_matched', match_confidence='high',
                date_updated=datetime('now')
            WHERE id=?
        """, (
            best.get('title'), best.get('author'), best.get('publish_year'),
            best.get('publisher'), best.get('cover_url'), best.get('subjects'),
            best.get('description'),
            best.get('external_id') if best['source'] == 'OpenLibrary' else None,
            best.get('external_id') if best['source'] == 'GoogleBooks' else None,
            lc_from_api or None,
            lc_parts.get('lc_class'), lc_parts.get('lc_number'),
            lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
            lc_parts.get('lc_sort'),
            book_id
        ))
        conn.commit()

    SCAN_STATUS["progress"] = len(needs_lookup)
    SCAN_STATUS["done"] = True
    SCAN_STATUS["running"] = False
    conn.close()


# ─── LC Re-extraction ────────────────────────────────────────────────────────

LC_STATUS = {"running": False, "done": False, "progress": 0, "total": 0, "current": ""}

def run_lc_reextract():
    """Re-extract LC call numbers from file contents for all books missing one."""
    global LC_STATUS
    LC_STATUS = {"running": True, "done": False, "progress": 0, "total": 0, "current": ""}

    # Fetch the work list then immediately close the connection
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("""
        SELECT b.id, bf.file_path, bf.file_ext, b.isbn13, b.isbn, b.openlibrary_id
        FROM books b
        LEFT JOIN book_files bf ON bf.book_id=b.id
        WHERE (b.lc_call_number IS NULL OR b.lc_call_number = '')
          AND (b.isbn13 IS NOT NULL OR b.isbn IS NOT NULL OR b.openlibrary_id IS NOT NULL)
          AND b.is_physical = 0
        GROUP BY b.id
    """).fetchall()]
    conn.close()

    LC_STATUS["total"] = len(rows)
    updated = 0

    for i, row in enumerate(rows):
        LC_STATUS["progress"] = i
        fname = row["file_path"] or ''
        LC_STATUS["current"] = os.path.basename(fname)

        # 1. Try extracting from file
        lc = None
        if fname and os.path.isfile(fname):
            try:
                _, _, lc = extract_isbn_from_file(fname)
            except Exception:
                pass

        # 2. Try OpenLibrary Works API if we have an OL ID (richest LC data)
        if not lc and row["openlibrary_id"]:
            try:
                ol_id = row["openlibrary_id"]
                works_data = http_get(f"https://openlibrary.org/works/{ol_id}.json")
                if works_data:
                    lc_list = works_data.get('lc_classifications', [])
                    if lc_list:
                        lc = lc_list[0].strip()
                if not lc:
                    eds = http_get(f"https://openlibrary.org/works/{ol_id}/editions.json?limit=5")
                    if eds:
                        for ed in eds.get('entries', []):
                            lc_list = ed.get('lc_classifications', [])
                            if lc_list:
                                lc = lc_list[0].strip()
                                break
            except Exception:
                pass

        # 2b. If no OL ID but we have an ISBN, query OpenLibrary by ISBN to find OL ID + LC
        new_ol_id = None
        if not lc:
            isbn = row["isbn13"] or row["isbn"]
            if isbn:
                try:
                    ol_candidates = query_openlibrary_isbn(isbn)
                    if ol_candidates:
                        ol_id = ol_candidates[0].get('external_id', '')
                        if ol_id:
                            new_ol_id = ol_id
                            works_data = http_get(f"https://openlibrary.org/works/{ol_id}.json")
                            if works_data:
                                lc_list = works_data.get('lc_classifications', [])
                                if lc_list:
                                    lc = lc_list[0].strip()
                            if not lc:
                                eds = http_get(f"https://openlibrary.org/works/{ol_id}/editions.json?limit=5")
                                if eds:
                                    for ed in eds.get('entries', []):
                                        lc_list = ed.get('lc_classifications', [])
                                        if lc_list:
                                            lc = lc_list[0].strip()
                                            break
                except Exception:
                    pass

        # 3. Try LOC SRU by ISBN
        if not lc:
            isbn = row["isbn13"] or row["isbn"]
            if isbn:
                try:
                    lc = query_loc_for_lc_number(isbn)
                except Exception:
                    pass

        # 4. Try OCLC Classify by ISBN
        if not lc:
            isbn = row["isbn13"] or row["isbn"]
            if isbn:
                try:
                    lc = query_oclc_classify_for_lc(isbn)
                except Exception:
                    pass

        # Write result — open a short-lived connection so we don't block other writers
        if lc or new_ol_id:
            try:
                wconn = sqlite3.connect(DB_PATH, timeout=15)
                wconn.execute("PRAGMA journal_mode=WAL")
                if new_ol_id:
                    wconn.execute(
                        "UPDATE books SET openlibrary_id=? WHERE id=? AND (openlibrary_id IS NULL OR openlibrary_id='')",
                        (new_ol_id, row["id"])
                    )
                if lc:
                    lc_parts = parse_lc(lc)
                    wconn.execute("""
                        UPDATE books SET
                            lc_call_number=?, lc_class=?, lc_number=?,
                            lc_cutter=?, lc_year=?, lc_sort=?,
                            date_updated=datetime('now')
                        WHERE id=?
                    """, (lc, lc_parts.get('lc_class'), lc_parts.get('lc_number'),
                          lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
                          lc_parts.get('lc_sort'), row["id"]))
                    updated += 1
                wconn.commit()
                wconn.close()
            except Exception:
                pass

    LC_STATUS["running"] = False
    LC_STATUS["done"] = True
    LC_STATUS["progress"] = len(rows)
    LC_STATUS["updated"] = updated

@app.route('/api/lc/reextract', methods=['POST'])
def api_lc_reextract():
    if LC_STATUS.get("running"):
        return jsonify({"error": "Already running"}), 400
    threading.Thread(target=run_lc_reextract, daemon=True).start()
    return jsonify({"ok": True})

@app.route('/api/lc/status')
def api_lc_status():
    return jsonify(LC_STATUS)


def try_merge_into_existing(db, book_id, isbn13):
    """
    If another book record already has isbn13, merge book_id into it:
    - Move all book_files rows from book_id to the existing record
    - Copy metadata from the richer record (prefer the one with more fields)
    - Delete match_candidates and the now-redundant book record
    Returns the surviving book_id, or None if no merge happened.
    """
    if not isbn13:
        return None
    existing = db.execute(
        "SELECT id FROM books WHERE isbn13=? AND id!=? AND is_physical=0",
        (isbn13, book_id)
    ).fetchone()
    if not existing:
        return None

    target_id = existing['id']

    # Move all files from book_id to target_id
    # (skip any that would violate UNIQUE on file_path — already there)
    db.execute(
        "UPDATE OR IGNORE book_files SET book_id=? WHERE book_id=?",
        (target_id, book_id)
    )
    # Delete any remaining book_files rows that couldn't move (exact duplicates)
    db.execute("DELETE FROM book_files WHERE book_id=?", (book_id,))

    # Merge metadata: fill in any blanks on target from source
    db.execute("""
        UPDATE books SET
            title        = COALESCE(NULLIF(title,''),        (SELECT title        FROM books WHERE id=?)),
            author       = COALESCE(NULLIF(author,''),       (SELECT author       FROM books WHERE id=?)),
            publish_year = COALESCE(NULLIF(publish_year,''), (SELECT publish_year FROM books WHERE id=?)),
            publisher    = COALESCE(NULLIF(publisher,''),    (SELECT publisher    FROM books WHERE id=?)),
            cover_url    = COALESCE(NULLIF(cover_url,''),    (SELECT cover_url    FROM books WHERE id=?)),
            subjects     = COALESCE(NULLIF(subjects,''),     (SELECT subjects     FROM books WHERE id=?)),
            lc_call_number = COALESCE(NULLIF(lc_call_number,''), (SELECT lc_call_number FROM books WHERE id=?)),
            lc_class     = COALESCE(NULLIF(lc_class,''),    (SELECT lc_class     FROM books WHERE id=?)),
            lc_number    = COALESCE(NULLIF(lc_number,''),   (SELECT lc_number    FROM books WHERE id=?)),
            lc_cutter    = COALESCE(NULLIF(lc_cutter,''),   (SELECT lc_cutter    FROM books WHERE id=?)),
            lc_year      = COALESCE(NULLIF(lc_year,''),     (SELECT lc_year      FROM books WHERE id=?)),
            lc_sort      = COALESCE(NULLIF(lc_sort,''),     (SELECT lc_sort      FROM books WHERE id=?)),
            match_status = CASE WHEN match_status IN ('confirmed','auto_matched')
                           THEN match_status
                           ELSE (SELECT match_status FROM books WHERE id=?) END,
            date_updated = datetime('now')
        WHERE id=?
    """, [book_id]*13 + [target_id])

    # Clean up the redundant record
    db.execute("DELETE FROM match_candidates WHERE book_id=?", (book_id,))
    db.execute("DELETE FROM books WHERE id=?", (book_id,))

    return target_id

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/cache/stats')
def api_cache_stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM api_cache").fetchone()[0]
    by_source = db.execute(
        "SELECT source, COUNT(*) as n FROM api_cache GROUP BY source ORDER BY source"
    ).fetchall()
    return jsonify({"total": total, "by_source": {r['source']: r['n'] for r in by_source}})

@app.route('/api/cache/clear', methods=['POST'])
def api_cache_clear():
    db = get_db()
    db.execute("DELETE FROM api_cache")
    db.commit()
    return jsonify({"ok": True})

@app.route('/api/google-key', methods=['GET'])
def api_get_google_key():
    key = _get_google_api_key()
    return jsonify({"key": key or ''})

@app.route('/api/google-key', methods=['POST'])
def api_set_google_key():
    global _GOOGLE_API_KEY
    data = request.json or {}
    key = data.get('key', '').strip()
    key_path = os.path.join(os.path.dirname(DB_PATH), 'google_api_key.txt')
    if key:
        with open(key_path, 'w') as f:
            f.write(key)
        _GOOGLE_API_KEY = key
    else:
        if os.path.isfile(key_path):
            os.remove(key_path)
        _GOOGLE_API_KEY = None
    return jsonify({"ok": True})

@app.route('/api/test-google')
def api_test_google():
    """Diagnostic: test Google Books API and return raw response info."""
    url = _google_url("https://www.googleapis.com/books/v1/volumes?q=intitle:hamlet&maxResults=1")
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'BookMeta/1.0'})
        with urllib.request.urlopen(req, timeout=6) as resp:
            raw = resp.read().decode('utf-8')
            data = json.loads(raw)
            items = len(data.get('items', []))
            return jsonify({"ok": True, "items": items, "has_key": bool(_get_google_api_key()),
                           "error": data.get('error', {}).get('message', '') if 'error' in data else ''})
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        return jsonify({"ok": False, "status": e.code, "body": body[:300], "has_key": bool(_get_google_api_key())})
    except Exception as ex:
        return jsonify({"ok": False, "error": str(ex), "has_key": bool(_get_google_api_key())})

@app.route('/api/stats')
def api_stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM books").fetchone()[0]
    matched = db.execute("SELECT COUNT(*) FROM books WHERE match_status != 'unmatched'").fetchone()[0]
    unmatched = db.execute("SELECT COUNT(*) FROM books WHERE match_status = 'unmatched'").fetchone()[0]
    needs_review = db.execute("SELECT COUNT(*) FROM books WHERE match_status = 'needs_review'").fetchone()[0]
    auto_matched = db.execute("SELECT COUNT(*) FROM books WHERE match_status = 'auto_matched'").fetchone()[0]
    confirmed = db.execute("SELECT COUNT(*) FROM books WHERE match_status = 'confirmed'").fetchone()[0]
    return jsonify({"total": total, "matched": matched, "unmatched": unmatched,
                    "needs_review": needs_review, "auto_matched": auto_matched, "confirmed": confirmed})

@app.route('/api/scan', methods=['POST'])
def api_scan():
    data = request.json
    path = data.get('path', '').strip()
    rescan = data.get('rescan', False)
    if not path or not os.path.isdir(path):
        return jsonify({"error": f"Directory not found: {path}"}), 400
    if SCAN_STATUS["running"]:
        return jsonify({"error": "Scan already running"}), 400
    t = threading.Thread(target=scan_library, args=(path, rescan), daemon=True)
    t.start()
    return jsonify({"ok": True})

@app.route('/api/scan/status')
def api_scan_status():
    return jsonify(SCAN_STATUS)

@app.route('/api/books')
def api_books():
    db = get_db()
    status = request.args.get('status', '')
    search = request.args.get('q', '')
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page

    where, params = [], []
    if status == 'no_isbn':
        where.append("(b.isbn13 IS NULL OR b.isbn13='')")
    elif status == 'no_lc':
        where.append("(b.lc_call_number IS NULL OR b.lc_call_number='')")
    elif status:
        where.append("b.match_status = ?")
        params.append(status)
    read_filter = request.args.get('read', '')
    if read_filter == 'yes':
        where.append("b.date_read IS NOT NULL")
    elif read_filter == 'no':
        where.append("b.date_read IS NULL")
    rating_filter = request.args.get('rating', '')
    if rating_filter.isdigit():
        where.append("b.rating = ?")
        params.append(int(rating_filter))
    lc_range = request.args.get('lc_range', '').strip().upper()
    if lc_range:
        # LC range browsing: "PQ2" -> PQ2xx, "HM" -> all HM, "PN8" -> PN8xx
        # Strategy: filter on lc_class match + numeric range on lc_number
        import re as _re
        _m = _re.match(r'([A-Z]{1,3})(\d*)', lc_range)
        if _m:
            cls, num_prefix = _m.group(1), _m.group(2)
            if not num_prefix:
                # Class only — "HM" matches all HM
                where.append("b.lc_class = ?")
                params.append(cls)
            else:
                # Class + number prefix — "BF4" -> lc_number starts with "4"
                # "BF41" -> lc_number starts with "41", "PQ2" -> starts with "2"
                where.append("b.lc_class = ? AND b.lc_number LIKE ?")
                params.extend([cls, num_prefix + '%'])
    if search:
        where.append("(b.title LIKE ? OR b.author LIKE ? OR EXISTS(SELECT 1 FROM book_files bf WHERE bf.book_id=b.id AND bf.file_name LIKE ?) OR b.isbn13 LIKE ? OR b.subjects LIKE ? OR b.lc_call_number LIKE ?)")
        params += [f'%{search}%'] * 6

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    # Sort
    sort = request.args.get('sort', 'date_added')
    sort_map = {
        'date_added': 'date_added DESC',
        'title':      'LOWER(COALESCE(b.title, (SELECT bf.file_name FROM book_files bf WHERE bf.book_id=b.id LIMIT 1))) ASC',
        'author':     'LOWER(COALESCE(b.author,"")) ASC, LOWER(COALESCE(b.title,"")) ASC',
        'year':       'b.publish_year DESC, LOWER(COALESCE(b.title,"")) ASC',
        'lc':         'COALESCE(b.lc_sort,"ZZZZZZ") ASC',
        'read':       'b.date_read DESC NULLS LAST',
        'rating':     'b.rating DESC NULLS LAST, LOWER(COALESCE(b.title,"")) ASC',
    }
    order_clause = sort_map.get(sort, 'date_added DESC')

    total = db.execute(f"SELECT COUNT(*) FROM books b {where_clause}", params).fetchone()[0]
    rows = db.execute(
        f"""SELECT b.*,
            (SELECT GROUP_CONCAT(bf.file_ext, ',') FROM book_files bf WHERE bf.book_id=b.id) as formats,
            (SELECT COUNT(*) FROM book_files bf WHERE bf.book_id=b.id) as file_count,
            COALESCE(b.primary_file_path,
                (SELECT bf.file_path FROM book_files bf WHERE bf.book_id=b.id LIMIT 1)) as file_path,
            (SELECT bf.file_name FROM book_files bf
             WHERE bf.book_id=b.id AND bf.file_path=COALESCE(b.primary_file_path,bf.file_path)
             LIMIT 1) as file_name,
            (SELECT bf.file_ext FROM book_files bf
             WHERE bf.book_id=b.id AND bf.file_path=COALESCE(b.primary_file_path,bf.file_path)
             LIMIT 1) as file_ext,
            (SELECT bf.file_size FROM book_files bf
             WHERE bf.book_id=b.id AND bf.file_path=COALESCE(b.primary_file_path,bf.file_path)
             LIMIT 1) as file_size
        FROM books b {where_clause} ORDER BY {order_clause} LIMIT ? OFFSET ?""",
        params + [per_page, offset]
    ).fetchall()
    
    return jsonify({
        "total": total,
        "page": page,
        "books": [dict(r) for r in rows]
    })

@app.route('/api/books/<int:book_id>')
def api_book(book_id):
    db = get_db()
    book = db.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
    if not book:
        return jsonify({"error": "Not found"}), 404
    candidates = db.execute("SELECT * FROM match_candidates WHERE book_id=?", (book_id,)).fetchall()
    files = db.execute("SELECT * FROM book_files WHERE book_id=? ORDER BY file_ext", (book_id,)).fetchall()
    return jsonify({
        "book": dict(book),
        "files": [dict(f) for f in files],
        "candidates": [dict(c) for c in candidates]
    })

@app.route('/api/books/<int:book_id>/apply', methods=['POST'])
def api_apply_match(book_id):
    data = request.json
    candidate_id = data.get('candidate_id')
    db = get_db()
    
    if candidate_id:
        c = db.execute("SELECT * FROM match_candidates WHERE id=? AND book_id=?", 
                       (candidate_id, book_id)).fetchone()
        if not c:
            return jsonify({"error": "Candidate not found"}), 404
        c = dict(c)
        raw = json.loads(c.get('raw_json', '{}'))
        
        # Parse LC if present in raw OpenLibrary data
        lc_raw = ''
        if c['source'] == 'OpenLibrary':
            cls_data = raw.get('classifications', {})
            lc_list = cls_data.get('lc_classifications', [])
            if lc_list:
                lc_raw = lc_list[0]
        lc_parts = parse_lc(lc_raw) if lc_raw else {}
        # First update this book's isbn13 so merge check works
        # Only set isbn13 if not already manually overridden
        isbn13_from_candidate = c.get('isbn') or ''
        book_current = db.execute("SELECT isbn13, manual_override FROM books WHERE id=?", (book_id,)).fetchone()
        if isbn13_from_candidate and not (book_current and book_current['manual_override'] and book_current['isbn13']):
            db.execute("UPDATE books SET isbn13=? WHERE id=?", (isbn13_from_candidate, book_id))

        subjects_val = _clean_subjects(raw.get('subjects', raw.get('categories', '')))

        try:
            db.execute("""
                UPDATE books SET
                    title=?, author=?, publish_year=?, publisher=?,
                    cover_url=?, subjects=?,
                    openlibrary_id=?, google_books_id=?,
                    lc_call_number=COALESCE(NULLIF(lc_call_number,''), ?),
                    lc_class=COALESCE(NULLIF(lc_class,''), ?),
                    lc_number=COALESCE(NULLIF(lc_number,''), ?),
                    lc_cutter=COALESCE(NULLIF(lc_cutter,''), ?),
                    lc_year=COALESCE(NULLIF(lc_year,''), ?),
                    lc_sort=COALESCE(NULLIF(lc_sort,''), ?),
                    match_status='confirmed', match_confidence='manual',
                    manual_override=1, date_updated=datetime('now')
                WHERE id=?
            """, (
                c['title'], c['author'], c['publish_year'], c['publisher'],
                c['cover_url'],
                subjects_val,
                c['external_id'] if c['source'] == 'OpenLibrary' else None,
                c['external_id'] if c['source'] == 'GoogleBooks' else None,
                lc_raw or None,
                lc_parts.get('lc_class'), lc_parts.get('lc_number'),
                lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
                lc_parts.get('lc_sort'),
                book_id
            ))
        except Exception as e:
            app.logger.error(f"apply candidate error: {e}")
            return jsonify({"error": str(e)}), 500
        # Check if another record already has this ISBN — merge if so
        merged_into = try_merge_into_existing(db, book_id, isbn13_from_candidate)
    else:
        # Manual update
        fields = ['title', 'author', 'publish_year', 'publisher', 'isbn13',
                  'lc_call_number', 'subjects', 'notes', 'description']
        updates = {f: data[f] for f in fields if f in data}
        merged_into = None
        if updates:
            # If lc_call_number is being set manually, parse it into components
            if 'lc_call_number' in updates and updates['lc_call_number']:
                lc_parts = parse_lc(updates['lc_call_number'])
                updates.update(lc_parts)
            sets = ', '.join(f"{k}=?" for k in updates)
            db.execute(
                f"UPDATE books SET {sets}, match_status='confirmed', manual_override=1, "
                f"date_updated=datetime('now') WHERE id=?",
                list(updates.values()) + [book_id]
            )
            # Check for merge on manual ISBN entry
            merged_into = try_merge_into_existing(db, book_id, updates.get('isbn13'))
    db.commit()
    if merged_into:
        return jsonify({"ok": True, "merged_into": merged_into})
    return jsonify({"ok": True})

@app.route('/api/books/<int:book_id>/status', methods=['POST'])
def api_set_status(book_id):
    data = request.json
    status = data.get('status')
    if status not in ('unmatched', 'needs_review', 'auto_matched', 'confirmed', 'skip'):
        return jsonify({"error": "Invalid status"}), 400
    db = get_db()
    db.execute("UPDATE books SET match_status=?, date_updated=datetime('now') WHERE id=?", 
               (status, book_id))
    db.commit()
    return jsonify({"ok": True})

@app.route('/api/books/<int:book_id>/lookup', methods=['POST'])
def api_lookup(book_id):
    """Re-run metadata lookup for a single book."""
    db = get_db()
    book = db.execute("SELECT * FROM books WHERE id=?", (book_id,)).fetchone()
    if not book:
        return jsonify({"error": "Not found"}), 404
    book = dict(book)
    
    data = request.json or {}
    isbn_override = data.get('isbn', '').strip()
    title_override = data.get('title', '').strip()
    
    isbn13 = isbn_override or book.get('isbn13')
    isbn10 = book.get('isbn')
    
    candidates = []
    if title_override:
        # Title override — search both sources, interleave results so both appear
        gb = query_google_books_title(title_override)
        ol = query_openlibrary_title(title_override)
        # Interleave: GB1, OL1, GB2, OL2, ...
        for i in range(max(len(gb), len(ol))):
            if i < len(gb): candidates.append(gb[i])
            if i < len(ol): candidates.append(ol[i])
        if isbn_override:
            candidates += query_openlibrary_isbn(isbn_override)
    elif isbn13:
        candidates += query_openlibrary_isbn(isbn13)
        candidates += query_google_books_isbn(isbn13)
    else:
        # No ISBN, no title override — fall back to filename
        candidates += query_google_books_title(book.get('file_name', '') or '')
    
    if candidates:
        db.execute("DELETE FROM match_candidates WHERE book_id=?", (book_id,))
        for c in candidates[:10]:
            db.execute("""
                INSERT INTO match_candidates
                (book_id, source, external_id, title, author, publish_year,
                 publisher, isbn, cover_url, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (book_id, c['source'], c['external_id'], c['title'], c['author'],
                  c['publish_year'], c['publisher'], c['isbn'], c['cover_url'], c['raw_json']))
        db.execute("UPDATE books SET match_status='needs_review' WHERE id=?", (book_id,))
        db.commit()
    
    return jsonify({"candidates": candidates})


@app.route('/api/books/<int:book_id>/open_location', methods=['POST'])
def api_open_location(book_id):
    """Open the folder containing the book's primary file."""
    import subprocess, platform
    db = get_db()
    data = request.json or {}
    file_id = data.get('file_id')
    if file_id:
        row = db.execute("SELECT file_path FROM book_files WHERE id=? AND book_id=?",
                         (file_id, book_id)).fetchone()
    else:
        book = db.execute("SELECT primary_file_path FROM books WHERE id=?", (book_id,)).fetchone()
        if not book:
            return jsonify({"error": "Not found"}), 404
        row = db.execute("SELECT file_path FROM book_files WHERE book_id=? LIMIT 1",
                         (book_id,)).fetchone()
    if not row:
        return jsonify({"error": "No file found"}), 404
    path = row['file_path']
    folder = os.path.dirname(path)
    try:
        system = platform.system()
        if system == 'Windows':
            # Explorer with file selected
            if os.path.isfile(path):
                subprocess.Popen(['explorer', '/select,', path])
            else:
                os.startfile(folder)
        elif system == 'Darwin':
            subprocess.Popen(['open', folder])
        else:
            subprocess.Popen(['xdg-open', folder])
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/books/<int:book_id>/open', methods=['POST'])
def api_open_book(book_id):
    """Open a book file. POST {"file_id": N} to open specific format, else opens primary."""
    import subprocess, platform
    db = get_db()
    data = request.json or {}
    file_id = data.get('file_id')
    if file_id:
        row = db.execute("SELECT file_path FROM book_files WHERE id=? AND book_id=?",
                         (file_id, book_id)).fetchone()
    else:
        book = db.execute("SELECT primary_file_path FROM books WHERE id=?", (book_id,)).fetchone()
        if not book:
            return jsonify({"error": "Not found"}), 404
        primary = book['primary_file_path']
        row = db.execute("SELECT file_path FROM book_files WHERE book_id=? AND file_path=?",
                         (book_id, primary)).fetchone() if primary else               db.execute("SELECT file_path FROM book_files WHERE book_id=? LIMIT 1", (book_id,)).fetchone()
    if not row:
        return jsonify({"error": "No file found"}), 404
    path = row['file_path']
    if not os.path.isfile(path):
        return jsonify({"error": f"File not found on disk: {path}"}), 404
    try:
        system = platform.system()
        if system == 'Windows':
            os.startfile(path)
        elif system == 'Darwin':
            subprocess.Popen(['open', path])
        else:
            subprocess.Popen(['xdg-open', path])
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/books/<int:book_id>/read', methods=['POST'])
def api_toggle_read(book_id):
    data = request.json or {}
    db = get_db()
    date_read = data.get('date_read')
    db.execute("UPDATE books SET date_read=?, date_updated=datetime('now') WHERE id=?",
               (date_read, book_id))
    db.commit()
    return jsonify({"ok": True, "date_read": date_read})

@app.route('/api/books/<int:book_id>/rate', methods=['POST'])
def api_rate_book(book_id):
    data = request.json or {}
    rating = data.get('rating')  # 1-5 or None to clear
    if rating is not None:
        rating = int(rating)
        if not 1 <= rating <= 5:
            return jsonify({"error": "Rating must be 1-5"}), 400
    db = get_db()
    db.execute("UPDATE books SET rating=?, date_updated=datetime(\'now\') WHERE id=?",
               (rating, book_id))
    db.commit()
    return jsonify({"ok": True, "rating": rating})

@app.route('/api/books/create', methods=['POST'])
def api_create_physical():
    """Create a placeholder entry for a physical book (no file)."""
    data = request.json or {}
    db = get_db()
    # Use a unique synthetic path so the UNIQUE constraint on file_path is satisfied
    isbn13 = data.get('isbn13', '').strip() or None
    isbn10 = data.get('isbn', '').strip() or None
    title  = data.get('title', '').strip() or None
    author = data.get('author', '').strip() or None
    lc_raw = data.get('lc_call_number', '').strip() or None
    lc_parts = parse_lc(lc_raw) if lc_raw else {}
    cur = db.execute("""
        INSERT INTO books
        (is_physical, isbn, isbn13, title, author, lc_call_number,
         lc_class, lc_number, lc_cutter, lc_year, lc_sort,
         match_status, manual_override)
        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unmatched', 0)
    """, (isbn10, isbn13, title, author, lc_raw,
          lc_parts.get('lc_class'), lc_parts.get('lc_number'),
          lc_parts.get('lc_cutter'), lc_parts.get('lc_year'),
          lc_parts.get('lc_sort')))
    book_id = cur.lastrowid
    db.commit()
    return jsonify({"ok": True, "id": book_id})

@app.route('/api/export')
def api_export():
    db = get_db()
    rows = db.execute("SELECT * FROM books ORDER BY author, title").fetchall()
    return jsonify([dict(r) for r in rows])



@app.route('/api/books/<int:book_id>/delete_with_file', methods=['POST'])
def api_delete_with_file(book_id):
    """Move file(s) to recycle bin and delete book record."""
    import platform, subprocess
    db = get_db()
    files = db.execute("SELECT file_path FROM book_files WHERE book_id=?", (book_id,)).fetchall()
    errors = []
    for f in files:
        path = f['file_path']
        if not os.path.isfile(path):
            continue
        try:
            if platform.system() == 'Windows':
                try:
                    from send2trash import send2trash
                    send2trash(path)
                except ImportError:
                    ps = ('Add-Type -AssemblyName Microsoft.VisualBasic; '
                          '[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteFile('
                          f'"{path}",'
                          '[Microsoft.VisualBasic.FileIO.UIOption]::OnlyErrorDialogs,'
                          '[Microsoft.VisualBasic.FileIO.RecycleOption]::SendToRecycleBin)')
                    subprocess.run(['powershell', '-Command', ps], capture_output=True)
            elif platform.system() == 'Darwin':
                subprocess.run(['osascript', '-e',
                    f'tell application "Finder" to delete POSIX file "{path}"'], capture_output=True)
            else:
                result = subprocess.run(['gio', 'trash', path], capture_output=True)
                if result.returncode != 0:
                    os.remove(path)
        except Exception as e:
            errors.append(str(e))
    db.execute("DELETE FROM match_candidates WHERE book_id=?", (book_id,))
    db.execute("DELETE FROM book_files WHERE book_id=?", (book_id,))
    db.execute("DELETE FROM books WHERE id=?", (book_id,))
    db.commit()
    return jsonify({"ok": True, "errors": errors})

@app.route('/api/books/<int:book_id>/delete', methods=['POST'])
def api_delete_book(book_id):
    db = get_db()
    db.execute("DELETE FROM match_candidates WHERE book_id=?", (book_id,))
    db.execute("DELETE FROM book_files WHERE book_id=?", (book_id,))
    db.execute("DELETE FROM books WHERE id=?", (book_id,))
    db.commit()
    return jsonify({"ok": True})

@app.route('/api/books/<int:book_id>/repath', methods=['POST'])
def api_repath_book(book_id):
    """Update a file's path in book_files. file_id required."""
    data = request.json
    new_path = data.get('new_path', '').strip()
    file_id  = data.get('file_id')
    if not new_path:
        return jsonify({"error": "No path provided"}), 400
    if not os.path.isfile(new_path):
        return jsonify({"error": f"File not found: {new_path}"}), 400
    db = get_db()
    p = Path(new_path)
    if file_id:
        db.execute(
            "UPDATE book_files SET file_path=?, file_name=?, file_ext=? WHERE id=? AND book_id=?",
            (new_path, p.name, p.suffix.lower(), file_id, book_id)
        )
    else:
        db.execute(
            "UPDATE book_files SET file_path=?, file_name=?, file_ext=? WHERE book_id=? LIMIT 1",
            (new_path, p.name, p.suffix.lower(), book_id)
        )
    db.execute("UPDATE books SET date_updated=datetime('now') WHERE id=?", (book_id,))
    db.commit()
    return jsonify({"ok": True})





if __name__ == '__main__':
    init_db()
    print("\n  BookMeta is running.")
    print("  Open your browser to: http://localhost:5001\n")
    app.run(host='127.0.0.1', port=5001, debug=False)
else:
    # Also run when imported/launched via wsgi or other means
    init_db()
