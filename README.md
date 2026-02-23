# BookMeta

I envisioned this as an Emby meets Calibre ebook library management platform for PC. It is a purely display layer like Emby that creates a record for each book file you have, automatically enriches that record by pulling data from the internet, and gives you a way to manually update records for books where no information can be automatically pulled. The purpose is to end up with a catalog built around the Library of Congress call number system to make it easier to browse your ebooks. This was built by AI. 

**BookMeta is a local web app that scans your ebook library, extracts ISBNs from file contents, and automatically enriches your collection with metadata from OpenLibrary, Google Books, and the Library of Congress — including LC call numbers for proper shelf classification.** It runs entirely on your machine with no cloud account required. To get started: install Python, run `pip install flask`, drop `BookMeta.bat` in the same folder as `app.py`, and double-click it.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Getting a Google Books API Key](#getting-a-google-books-api-key)
- [How It Works](#how-it-works)
- [Using BookMeta](#using-bookmeta)
- [File Format Support](#file-format-support)
- [Data & Privacy](#data--privacy)

---

## Features

**Library Management**
- Scans a folder (and all subfolders) for ebook files and indexes them into a local SQLite database
- Automatically detects new files on rescan without re-processing already-indexed books
- Detects moved files by SHA1 hash — renames and folder reorganization don't create duplicates
- Cleans up records for deleted files automatically on every scan
- Add physical books (no file) manually for a complete catalog

**Metadata Enrichment**
- Extracts ISBNs directly from EPUB, PDF, MOBI, and AZW file contents (copyright page scanning, OCR fallback)
- Queries OpenLibrary, Google Books, LOC SRU, and OCLC Classify to find title, author, publisher, year, subjects, cover image, and LC call number
- All API responses cached locally — each ISBN is only ever looked up once (30-day TTL)
- Re-lookup any book by title or ISBN override if the automatic match was wrong

**LC Call Number Support**
- Extracts LC call numbers from file contents where present (CIP data)
- Falls back to four external sources: OpenLibrary Works API → LOC SRU → OCLC Classify
- "Find Missing LC Numbers" button batch-processes all matched books that still lack an LC number
- LC call number browser — filter by class and subclass (e.g. `PS`, `BF41`, `HM`)
- Proper LC sort order in the list view

**Match Workflow**
- Books start as `Unmatched` → enrichment moves them to `Auto Matched` → you confirm or correct
- Apply a match candidate with one click to lock in the metadata
- Manual edits via the Save button set a `manual_override` flag — those books are never touched by rescans or re-enrichment
- Flag books for review, mark as skip, or leave unmatched
- Merge duplicate records when two files turn out to be the same book

**Reading & Rating**
- Launch the books directly from the program
- Mark books as read with a date
- 1–5 star ratings
- Filter list by read status or rating

**Search & Filter**
- Full-text search across title, author, subjects, ISBN, and filename
- Filter by match status, read status, rating, or LC range
- Sort by date added, title, author, or year

**Housekeeping**
- Delete a book record only, or delete the file to Recycle Bin at the same time
- Open the containing folder for any book directly from the UI
- Export the full catalog to CSV

---

## Requirements

- **Python 3.8+** — [python.org/downloads](https://www.python.org/downloads/)
- **Flask** — `pip install flask`
- **PDF support** (optional but recommended, for ISBN extraction from PDFs):
  - PyMuPDF: `pip install pymupdf` *(faster, preferred)*
  - or pdfminer: `pip install pdfminer.six` *(fallback)*
- **Recycle Bin support** (optional, for safe file deletion):
  - `pip install send2trash`

Install everything at once:
```
pip install flask pymupdf send2trash
```

No other dependencies. OpenLibrary, LOC SRU, and OCLC Classify are all free with no account required. Only Google Books benefits from an API key (see below).

---

## Installation

1. **Download** the latest release and unzip it anywhere — e.g. `C:\Program Files\bookmeta\`

2. **Install Python** if you haven't already. During installation, check **"Add Python to PATH"**.

3. **Install dependencies** by opening a terminal and running:
   ```
   pip install flask pymupdf send2trash
   ```

4. **Launch the app** by double-clicking `BookMeta.bat`. A terminal window will open and your browser will navigate to `http://localhost:5001` automatically.

5. **Enter your library path** in the sidebar (e.g. `K:\eBooks\Book Files`) and click **Scan Folder**.

The first scan indexes all files immediately (Phase 1), then begins enriching books with metadata in the background (Phase 2). You can browse and work with your library while enrichment is running.

Your database (`books.db`) is created automatically in the same folder as `app.py` and persists between sessions. Closing the terminal window stops the server.

---

## Getting a Google Books API Key

Google Books works without an API key but is subject to shared rate limits that run out quickly on large libraries (typically ~200 unauthenticated requests per day across all users). With your own free API key the daily limit is 1,000 requests.

1. Go to [console.cloud.google.com](https://console.cloud.google.com) and sign in with a Google account
2. Create a new project (or select an existing one)
3. Click **"Enable APIs and Services"** and search for **"Books API"** — enable it
4. Go to **"Credentials"** → **"Create Credentials"** → **"API Key"**
5. Copy the key (starts with `AIza...`)
6. In BookMeta, paste it into the **Google Books API Key** field in the Settings section of the sidebar and click **Save**
7. Click **Test Google Books** to verify it's working

The key is saved to `google_api_key.txt` in the app folder and loaded automatically on startup.

---

## How It Works

### Scanning

BookMeta scans your library in three phases:

**Phase 0 — Cleanup:** Compares the current files on disk against the database. Files that have moved (detected by SHA1 hash) get their paths updated. Files that are gone get their records removed. This runs on every scan.

**Phase 1 — Indexing:** Walks the folder tree and registers every ebook file. For EPUB and MOBI files, it opens the archive and scans the copyright page for an ISBN. For PDFs it extracts text from the first few pages. This phase is fast and entirely local — books appear in the UI immediately.

**Phase 2 — Enrichment:** For each book with an ISBN, queries OpenLibrary and Google Books for metadata. The best match is stored and the book is marked `Auto Matched`. Books without ISBNs are left `Unmatched` for manual review. This phase runs in the background and can take a while for large libraries.

### API Caching

Every API response is cached in the local database for 30 days. If you rescan, run "Find Missing LC Numbers", or re-look up a book, no network request is made for any ISBN that was already queried. This means after the initial scan, subsequent operations are nearly instant and don't count against your API quota.

### Match Status

| Status | Meaning |
|--------|---------|
| `Unmatched` | No ISBN found or enrichment failed |
| `Needs Review` | Flagged for manual attention |
| `Auto Matched` | Enriched automatically, not yet confirmed |
| `Confirmed` | You applied a candidate or saved manually — protected from future overwrites |

Books with `Confirmed` status or the `manual_override` flag are never modified by rescans or enrichment. Your manual edits are permanent.

### LC Call Numbers

LC numbers are sourced in order of reliability:
1. Extracted from the file itself (CIP data in copyright page)
2. OpenLibrary Works API (using stored `openlibrary_id`)
3. Library of Congress SRU endpoint (by ISBN)
4. OCLC Classify (by ISBN)

The "Find Missing LC Numbers" button runs all four sources for every matched book that still lacks an LC number. This is useful after a batch of manual title lookups that populated ISBNs but not LC data.

---

## Using BookMeta

### First Scan
Enter your library folder path and click **Scan Folder**. The progress bar shows Phase 1 (indexing) and Phase 2 (enriching) separately. You can browse your library immediately while enrichment runs in the background.

### Working the Unmatched Queue
Click **Unmatched** in the filter sidebar to see books without metadata. Click a book to open the detail panel on the right. If an ISBN was extracted, candidates are shown automatically. Click a candidate to apply it. If the candidates are wrong, type a title or ISBN in the Re-Lookup fields and click **Search Metadata Sources**.

### Manual Edits
Click any field in the detail panel to edit it directly. Click **Save** to write the changes. Saved books get the `manual_override` flag and will never be touched by automatic enrichment again.

### Finding Missing LC Numbers
After enrichment completes, some matched books may still lack LC call numbers (Google Books often doesn't include them). Click **Find Missing LC Numbers** in the sidebar to batch-query LOC and OCLC for all books that have an ISBN but no LC. This runs in the background and can be left to run on its own.

### Rescan
**Scan Folder** processes only new or changed files. **Rescan All** re-runs enrichment on all unmatched books (safe to run — confirmed/manual books are skipped). If you hit Google's daily API limit mid-scan, run Rescan All the next day and it will pick up where it left off — already-enriched books are skipped, and already-cached API responses don't count against your quota.

---

## File Format Support

| Format | ISBN Extraction | Notes |
|--------|----------------|-------|
| `.epub` | ✅ | Scans OPF metadata and copyright page HTML |
| `.pdf` | ✅ | Requires PyMuPDF or pdfminer (see Requirements) |
| `.mobi` / `.azw` | ✅ | Reads PalmDB binary format |
| `.azw3` | ✅ | |
| `.djvu` | Indexed only | No text extraction |
| `.cbz` / `.cbr` | Indexed only | Comic archives |
| `.tif` / `.tiff` | Indexed only | |
| `.mp3`, `.m4b`, etc. | Indexed only | Audiobooks tracked by filename |

---

## Data & Privacy

All data is stored locally in `books.db` (SQLite) in the application folder. No data is sent to any server except the metadata API queries (OpenLibrary, Google Books, LOC, OCLC) which only receive ISBNs and book titles. No account, login, or internet connection is required to use the app — only for metadata enrichment.
The Google Books API key is stored in plain text alongside the database file. 

The database file can be backed up by simply copying `books.db`. It is safe to replace `app.py` and the `templates` folder with a newer version without affecting your data.
