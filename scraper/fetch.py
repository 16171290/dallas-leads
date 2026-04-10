"""
Dallas County, Texas — Motivated Seller Lead Scraper
=====================================================
Scrapes the Dallas County Clerk online records portal (dcrecords.dallascounty.org)
for the last 7 days of filings across all motivated-seller document types.
Enriches records with owner/address data from the Dallas CAD bulk parcel DBF.
Scores each lead 0–100 and writes JSON + CSV outputs.

Author  : Dallas Lead Scraper (automated)
Run     : python scraper/fetch.py
Schedule: 07:00 UTC daily via GitHub Actions
"""

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False
    logging.warning("dbfread not installed – parcel enrichment disabled")

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("dallas_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = 7

# The actual records search engine used by Dallas County Clerk
SEARCH_BASE = "https://dallas.tx.publicsearch.us"
SEARCH_URL  = f"{SEARCH_BASE}/search/advanced"

# Dallas CAD bulk data page
CAD_PAGE = "https://www.dallascad.org/DataProducts.aspx"

# Doc type → (category_code, human_label)
DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("LP",       "Lis Pendens"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    "JUD":      ("JUD",      "Judgment"),
    "CCJ":      ("CCJ",      "Certified Judgment"),
    "DRJUD":    ("DRJUD",    "Domestic Relations Judgment"),
    "LNCORPTX": ("LNCORPTX", "Corp Tax Lien"),
    "LNIRS":    ("LNIRS",    "IRS Lien"),
    "LNFED":    ("LNFED",    "Federal Lien"),
    "LN":       ("LN",       "Lien"),
    "LNMECH":   ("LNMECH",   "Mechanic Lien"),
    "LNHOA":    ("LNHOA",    "HOA Lien"),
    "MEDLN":    ("MEDLN",    "Medicaid Lien"),
    "PRO":      ("PRO",      "Probate"),
    "NOC":      ("NOC",      "Notice of Commencement"),
}
ALL_CODES = list(DOC_TYPE_MAP.keys())

# Motivated-seller flag definitions (label, predicate)
FLAG_DEFS: list[tuple[str, callable]] = [
    ("Lis pendens",      lambda r: r["cat"] == "LP"),
    ("Pre-foreclosure",  lambda r: r["cat"] in ("NOFC", "TAXDEED")),
    ("Judgment lien",    lambda r: r["cat"] in ("JUD", "CCJ", "DRJUD")),
    ("Tax lien",         lambda r: r["cat"] in ("LNCORPTX", "LNIRS", "LNFED")),
    ("Mechanic lien",    lambda r: r["cat"] == "LNMECH"),
    ("Probate / estate", lambda r: r["cat"] == "PRO"),
    ("LLC / corp owner", lambda r: bool(re.search(
        r'\b(LLC|INC|CORP|LP|LTD|TRUST|ESTATE)\b',
        (r.get("owner") or ""), re.I))),
    ("New this week",    lambda r: True),  # all fetched records are ≤7 days old
]

# ─────────────────────────────────────────────────────────────────────────────
# Output paths
# ─────────────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR      = ROOT / "data"
for d in (DASHBOARD_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

DASHBOARD_JSON = DASHBOARD_DIR / "records.json"
DATA_JSON      = DATA_DIR      / "records.json"
GHL_CSV        = DATA_DIR      / "ghl_export.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_amount(text: str) -> Optional[float]:
    """Extract a numeric dollar amount from a string."""
    if not text:
        return None
    clean = re.sub(r"[^0-9.]", "", str(text))
    try:
        v = float(clean)
        return v if v > 0 else None
    except ValueError:
        return None


def name_variants(full: str) -> list[str]:
    """
    Return owner-name lookup variants:
      'JOHN SMITH'  →  ['JOHN SMITH', 'SMITH JOHN', 'SMITH, JOHN']
    Handles multi-word last names reasonably.
    """
    full = full.strip().upper()
    full = re.sub(r"\s+", " ", full)
    parts = full.split()
    if not parts:
        return [full]
    if len(parts) == 1:
        return [full]
    first, last = parts[0], parts[-1]
    return [
        full,
        f"{last} {first}",
        f"{last}, {first}",
        " ".join(parts),          # original
    ]


def safe_retry(fn, attempts: int = 3, delay: float = 5.0, *args, **kwargs):
    """Synchronous retry wrapper.  Returns None on all failures."""
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning(f"  Retry {i+1}/{attempts} – {exc}")
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


def parse_date(s: str) -> Optional[datetime]:
    """Try several common date formats."""
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y",
                "%B %d, %Y", "%b %d, %Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Dallas CAD Parcel Lookup
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    """
    Downloads the Dallas CAD bulk parcel DBF (or ZIP containing it),
    and builds an owner-name → address index for fast lookup.

    Column name variants supported:
      Owner    : OWNER, OWN1
      Site addr: SITE_ADDR, SITEADDR
      Site city: SITE_CITY
      Site zip : SITE_ZIP
      Mail addr: ADDR_1, MAILADR1
      Mail city: CITY, MAILCITY
      Mail state: STATE
      Mail zip : ZIP, MAILZIP
    """

    def __init__(self):
        self._index: dict[str, dict] = {}

    # ── public ──────────────────────────────────────────────────────────────

    def build(self):
        log.info("Fetching Dallas CAD bulk parcel data …")
        dbf_bytes = self._download()
        if dbf_bytes:
            self._parse(dbf_bytes)
            log.info(f"Parcel index ready: {len(self._index):,} owner entries")
        else:
            log.warning("Parcel data unavailable – address enrichment will be skipped")

    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        for v in name_variants(owner):
            hit = self._index.get(v)
            if hit:
                return hit
        return {}

    # ── private ─────────────────────────────────────────────────────────────

    def _download(self) -> Optional[bytes]:
        session = requests.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        )

        # ── Step 1: fetch the DataProducts page ──
        try:
            resp = safe_retry(session.get, 3, 5, CAD_PAGE, timeout=30)
            if resp is None or not resp.ok:
                raise ValueError("Could not load CAD page")
        except Exception as e:
            log.warning(f"CAD page unavailable: {e}")
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        # ── Step 2: try direct href links (.zip / .dbf / .csv) ──
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(href.lower().endswith(ext) for ext in (".zip", ".dbf", ".csv")):
                url = urljoin(CAD_PAGE, href)
                log.info(f"Trying CAD direct download: {url}")
                try:
                    dl = session.get(url, timeout=120)
                    dl.raise_for_status()
                    result = self._extract_dbf(dl.content)
                    if result:
                        return result
                except Exception as e:
                    log.warning(f"  Direct link failed: {e}")

        # ── Step 3: ASP.NET __doPostBack ──
        vs  = (soup.find("input", {"id": "__VIEWSTATE"})          or {}).get("value", "")
        ev  = (soup.find("input", {"id": "__EVENTVALIDATION"})    or {}).get("value", "")
        vsg = (soup.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value", "")

        event_targets = [
            "lnkResidential",
            "lnkCommercial",
            "lnkAllParcels",
            "ctl00$ContentPlaceHolder1$lnkResidential",
            "ctl00$ContentPlaceHolder1$lnkCommercial",
            "ctl00$ContentPlaceHolder1$lnkAllParcels",
            "btnDownload",
            "ctl00$ContentPlaceHolder1$btnDownload",
        ]
        for target in event_targets:
            payload = {
                "__VIEWSTATE":          vs,
                "__EVENTVALIDATION":    ev,
                "__VIEWSTATEGENERATOR": vsg,
                "__EVENTTARGET":        target,
                "__EVENTARGUMENT":      "",
            }
            try:
                dl = session.post(CAD_PAGE, data=payload, timeout=120)
                if dl.ok and len(dl.content) > 2_000:
                    result = self._extract_dbf(dl.content)
                    if result:
                        log.info(f"Got CAD data via __doPostBack({target})")
                        return result
            except Exception as e:
                log.debug(f"doPostBack {target}: {e}")

        log.warning("Could not obtain CAD parcel file via any method.")
        return None

    def _extract_dbf(self, content: bytes) -> Optional[bytes]:
        # ZIP archive?
        if content[:2] == b"PK":
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            log.info(f"Extracted DBF from ZIP: {name}")
                            return zf.read(name)
            except Exception as e:
                log.warning(f"ZIP extract error: {e}")
            return None
        # Raw DBF (magic bytes 0x03 / 0x83 / 0xF5)?
        if content[:1] in (b"\x03", b"\x83", b"\xf5"):
            return content
        return None

    def _parse(self, dbf_bytes: bytes):
        if not HAS_DBF:
            log.warning("dbfread not available – skipping parcel parse")
            return
        tmp = Path("/tmp/_dallas_parcels.dbf")
        tmp.write_bytes(dbf_bytes)
        try:
            tbl = DBF(str(tmp), lowernames=True, ignore_missing_memofile=True,
                      encoding="latin-1")
            for row in tbl:
                try:
                    owner = str(
                        row.get("owner") or row.get("own1") or ""
                    ).strip().upper()
                    if not owner:
                        continue

                    def g(*keys):
                        for k in keys:
                            v = row.get(k)
                            if v:
                                return str(v).strip()
                        return ""

                    parcel = {
                        "prop_address": g("site_addr", "siteaddr").title(),
                        "prop_city":    g("site_city").title(),
                        "prop_state":   "TX",
                        "prop_zip":     g("site_zip", "zip"),
                        "mail_address": g("addr_1", "mailadr1").title(),
                        "mail_city":    g("city", "mailcity").title(),
                        "mail_state":   g("state").upper() or "TX",
                        "mail_zip":     g("zip", "mailzip"),
                    }
                    for variant in name_variants(owner):
                        self._index.setdefault(variant, parcel)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"DBF parse error: {e}")
        finally:
            tmp.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Dallas County Clerk Scraper (Playwright)
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Uses Playwright (async Chromium) to search dallas.tx.publicsearch.us
    for each document type code over the configured date range.

    The portal is a legacy ASP.NET application.  Field names observed:
      InstruType  – document type code
      BeginDate   – MM/DD/YYYY
      EndDate     – MM/DD/YYYY
    Results are rendered in HTML tables.
    """

    MAX_PAGES = 25  # safety limit per doc-type search

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.records: list[dict] = []

    # ── public ──────────────────────────────────────────────────────────────

    async def run(self):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx  = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = await ctx.new_page()

            for code in ALL_CODES:
                log.info(f"Clerk search → {code}")
                try:
                    recs = await self._search(page, code)
                    log.info(f"  {len(recs)} records")
                    self.records.extend(recs)
                except Exception as exc:
                    log.error(f"  Error fetching {code}: {exc}")
                    traceback.print_exc()
                await asyncio.sleep(1.5)

            await browser.close()

        log.info(f"Playwright scrape complete – {len(self.records)} raw records")

    # ── private ─────────────────────────────────────────────────────────────

    async def _search(self, page, code: str) -> list[dict]:
        records  = []
        start_s  = self.start.strftime("%m/%d/%Y")
        end_s    = self.end.strftime("%m/%d/%Y")

        # Load search page (with retry)
        loaded = False
        for attempt in range(3):
            try:
                await page.goto(SEARCH_URL, timeout=30_000, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=20_000)
                loaded = True
                break
            except PWTimeout:
                log.warning(f"  Page load timeout (attempt {attempt+1})")
                await asyncio.sleep(3)
        if not loaded:
            return records

        # Inspect form fields present on page
        html_before = await page.content()
        soup_before = BeautifulSoup(html_before, "lxml")

        # Fill instrument/doc-type field
        type_filled = False
        for selector, attr in [
            ('input[name="InstruType"]',  "fill"),
            ('input[name="instrtype"]',   "fill"),
            ('input[name="DocType"]',     "fill"),
            ('select[name="InstruType"]', "select"),
            ('select[name="instrtype"]',  "select"),
        ]:
            if soup_before.select(selector):
                try:
                    if attr == "fill":
                        await page.fill(selector, code)
                    else:
                        await page.select_option(selector, value=code)
                    type_filled = True
                    break
                except Exception:
                    pass
        if not type_filled:
            log.warning(f"  Could not set doc type for {code}")

        # Fill date range
        for start_sel, end_sel in [
            ('input[name="BeginDate"]',  'input[name="EndDate"]'),
            ('input[name="begindate"]',  'input[name="enddate"]'),
            ('input[name="StartDate"]',  'input[name="EndDate"]'),
            ('#BeginDate',               '#EndDate'),
        ]:
            if soup_before.select(start_sel):
                try:
                    await page.fill(start_sel, start_s)
                    await page.fill(end_sel,   end_s)
                    break
                except Exception:
                    pass

        # Submit form
        try:
            await page.click(
                'input[type="submit"], button[type="submit"], '
                'input[value="Search"], button:has-text("Search")',
                timeout=8_000,
            )
            await page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception as e:
            log.warning(f"  Submit error for {code}: {e}")
            return records

        # Paginate results
        page_num = 0
        while page_num < self.MAX_PAGES:
            page_num += 1
            html     = await page.content()
            recs     = self._parse_results(html, code, page.url)
            records.extend(recs)

            # Look for "next page" navigation
            soup = BeautifulSoup(html, "lxml")
            next_a = (
                soup.find("a", string=re.compile(r"next|»|>", re.I)) or
                soup.find("a", attrs={"title": re.compile(r"next", re.I)})
            )
            if not next_a:
                break
            href = next_a.get("href", "")
            if not href:
                break
            next_url = urljoin(SEARCH_BASE, href)
            try:
                await page.goto(next_url, timeout=20_000, wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception as e:
                log.warning(f"  Pagination error: {e}")
                break

        return records

    # ── HTML parser ──────────────────────────────────────────────────────────

    def _parse_results(self, html: str, code: str, current_url: str) -> list[dict]:
        records   = []
        soup      = BeautifulSoup(html, "lxml")
        cat, lbl  = DOC_TYPE_MAP.get(code, (code, code))

        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            # Build header map
            hdr_cells = rows[0].find_all(["th", "td"])
            headers   = [c.get_text(strip=True).lower() for c in hdr_cells]

            if not any(
                kw in " ".join(headers)
                for kw in ["instr", "doc", "filed", "record", "grantor", "grantee"]
            ):
                continue

            col: dict[str, int] = {}
            for i, h in enumerate(headers):
                if re.search(r"instr|doc.?num|number|num", h) and "doc_num" not in col:
                    col["doc_num"] = i
                if re.search(r"type", h) and "doc_type" not in col:
                    col["doc_type"] = i
                if re.search(r"filed|date|record", h) and "filed" not in col:
                    col["filed"] = i
                if re.search(r"grantor|owner", h) and "grantor" not in col:
                    col["grantor"] = i
                if re.search(r"grantee", h) and "grantee" not in col:
                    col["grantee"] = i
                if re.search(r"legal|desc", h) and "legal" not in col:
                    col["legal"] = i
                if re.search(r"amount|consid|value", h) and "amount" not in col:
                    col["amount"] = i

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                try:
                    def ct(key: str) -> str:
                        idx = col.get(key)
                        return cells[idx].get_text(strip=True) if (idx is not None and idx < len(cells)) else ""

                    # Prefer doc link from first <a> in row
                    link_tag = row.find("a", href=True)
                    clerk_url = ""
                    if link_tag:
                        h = link_tag["href"]
                        clerk_url = urljoin(SEARCH_BASE, h) if h.startswith("/") else h

                    doc_num = ct("doc_num") or (link_tag.get_text(strip=True) if link_tag else "")
                    if not doc_num:
                        continue

                    filed_raw = ct("filed")
                    dt        = parse_date(filed_raw)
                    filed_iso = dt.strftime("%Y-%m-%d") if dt else filed_raw

                    records.append({
                        "doc_num":      doc_num.strip(),
                        "doc_type":     code,
                        "filed":        filed_iso,
                        "cat":          cat,
                        "cat_label":    lbl,
                        "owner":        ct("grantor").strip(),
                        "grantee":      ct("grantee").strip(),
                        "amount":       parse_amount(ct("amount")),
                        "legal":        ct("legal").strip(),
                        "clerk_url":    clerk_url or current_url,
                        "prop_address": "",
                        "prop_city":    "Dallas",
                        "prop_state":   "TX",
                        "prop_zip":     "",
                        "mail_address": "",
                        "mail_city":    "",
                        "mail_state":   "",
                        "mail_zip":     "",
                        "flags":        [],
                        "score":        0,
                    })
                except Exception as exc:
                    log.debug(f"Row parse error: {exc}")

        return records


# ─────────────────────────────────────────────────────────────────────────────
# REST / JSON Fallback Scraper
# ─────────────────────────────────────────────────────────────────────────────

class ClerkRestFallback:
    """
    Tries known Dallas County REST API endpoints (discovered from network
    inspection of the clerk portal) when Playwright returns 0 records.
    """

    ENDPOINTS = [
        # These are the endpoints observed behind the clerk portal SPA
        f"{SEARCH_BASE}/search/SearchResults.php",
        f"{SEARCH_BASE}/api/search",
        "https://www.dallascounty.org/apps/ClerkAPI/search",
    ]

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 Dallas-Lead-Scraper/1.0",
            "Accept":     "application/json, text/html, */*",
            "Referer":    SEARCH_URL,
        })

    def fetch_all(self) -> list[dict]:
        records: list[dict] = []
        for code in ALL_CODES:
            for ep in self.ENDPOINTS:
                try:
                    recs = self._fetch(ep, code)
                    if recs:
                        log.info(f"  REST fallback {code}: {len(recs)} from {ep}")
                        records.extend(recs)
                        break
                except Exception as e:
                    log.debug(f"  REST {ep} {code}: {e}")
        return records

    def _fetch(self, endpoint: str, code: str) -> list[dict]:
        params = {
            "InstruType": code,
            "BeginDate":  self.start.strftime("%m/%d/%Y"),
            "EndDate":    self.end.strftime("%m/%d/%Y"),
            "format":     "json",
        }
        r = self.session.get(endpoint, params=params, timeout=20)
        r.raise_for_status()
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            return self._norm(r.json(), code)
        # HTML fallback
        return ClerkScraper(self.start, self.end)._parse_results(r.text, code, endpoint)

    def _norm(self, data, code: str) -> list[dict]:
        cat, lbl = DOC_TYPE_MAP.get(code, (code, code))
        items    = data if isinstance(data, list) else (
            data.get("results") or data.get("records") or []
        )
        out: list[dict] = []
        for it in items:
            try:
                out.append({
                    "doc_num":      str(it.get("InstrumentNumber") or it.get("doc_num") or ""),
                    "doc_type":     code,
                    "filed":        str(it.get("FileDate") or it.get("filed") or ""),
                    "cat":          cat,
                    "cat_label":    lbl,
                    "owner":        str(it.get("Grantor") or it.get("owner") or ""),
                    "grantee":      str(it.get("Grantee") or it.get("grantee") or ""),
                    "amount":       parse_amount(str(it.get("Amount") or "")),
                    "legal":        str(it.get("LegalDescription") or it.get("legal") or ""),
                    "clerk_url":    str(it.get("url") or it.get("link") or ""),
                    "prop_address": "", "prop_city": "Dallas",
                    "prop_state": "TX", "prop_zip": "",
                    "mail_address": "", "mail_city": "",
                    "mail_state": "", "mail_zip": "",
                    "flags": [], "score": 0,
                })
            except Exception:
                pass
        return out


# ─────────────────────────────────────────────────────────────────────────────
# Scoring Engine
# ─────────────────────────────────────────────────────────────────────────────

def score_record(record: dict, all_records: list[dict]) -> dict:
    """
    Compute motivated-seller flags and score (0–100).

    Scoring rubric
    ──────────────
    Base                             30
    +10 per flag                     variable
    +20 LP + FC combo (same owner)   bonus
    +15 amount > $100 k              one-time
    +10 amount > $50 k               (else)
    +5  new this week                always (all records qualify)
    +5  has property/mail address    quality bonus
    Cap                             100
    """
    flags: list[str] = []
    for label, predicate in FLAG_DEFS:
        try:
            if predicate(record):
                flags.append(label)
        except Exception:
            pass

    score = 30 + 10 * len(flags)

    # LP + foreclosure combo bonus (check sibling records for same owner)
    owner = (record.get("owner") or "").strip().upper()
    if owner:
        sibling_cats = {r["cat"] for r in all_records
                        if (r.get("owner") or "").strip().upper() == owner}
        if "LP" in sibling_cats and sibling_cats & {"NOFC", "TAXDEED"}:
            score += 20

    # Amount bonus
    amt = record.get("amount") or 0
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10

    # Has address
    if record.get("prop_address") or record.get("mail_address"):
        score += 5

    record["flags"] = flags
    record["score"] = min(int(score), 100)
    return record


# ─────────────────────────────────────────────────────────────────────────────
# GHL Export
# ─────────────────────────────────────────────────────────────────────────────

GHL_FIELDS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def split_name(full: str) -> tuple[str, str]:
    parts = re.split(r"\s+", (full or "").strip())
    if not parts or not parts[0]:
        return "", ""
    first = parts[0].title()
    last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
    return first, last


def write_ghl_csv(records: list[dict], path: Path):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in records:
            first, last = split_name(r.get("owner", ""))
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", ""),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", ""),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       r.get("amount") or "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Dallas County Clerk (dallas.tx.publicsearch.us)",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV → {path}  ({len(records)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    now      = datetime.now(timezone.utc)
    end_dt   = now
    start_dt = now - timedelta(days=LOOKBACK_DAYS)

    log.info("=" * 65)
    log.info("  Dallas County TX – Motivated Seller Lead Scraper")
    log.info(f"  Range : {start_dt.date()} → {end_dt.date()}")
    log.info("=" * 65)

    # ── 1. Build parcel index ───────────────────────────────────────────────
    parcel = ParcelLookup()
    parcel.build()

    # ── 2. Scrape clerk portal ──────────────────────────────────────────────
    clerk = ClerkScraper(start_dt, end_dt)
    await clerk.run()
    records = clerk.records

    # ── 2b. REST fallback if playwright came up empty ───────────────────────
    if not records:
        log.warning("No records from Playwright – trying REST fallback …")
        records = ClerkRestFallback(start_dt, end_dt).fetch_all()

    log.info(f"Raw records : {len(records)}")

    # ── 3. Deduplicate on doc_num + doc_type ────────────────────────────────
    seen: set[str] = set()
    unique: list[dict] = []
    for r in records:
        key = f"{r.get('doc_num','').strip()}|{r.get('doc_type','')}"
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    log.info(f"After dedup : {len(unique)}")

    # ── 4. Parcel enrichment ────────────────────────────────────────────────
    with_addr = 0
    for r in unique:
        hit = parcel.lookup(r.get("owner", ""))
        if hit:
            r.update(hit)
            with_addr += 1
    log.info(f"With address: {with_addr}")

    # ── 5. Score every lead ─────────────────────────────────────────────────
    for r in unique:
        score_record(r, unique)

    # Sort best leads first
    unique.sort(key=lambda x: x.get("score", 0), reverse=True)

    # ── 6. Build JSON payload ───────────────────────────────────────────────
    payload = {
        "fetched_at": now.isoformat(),
        "source":     "Dallas County Clerk – dallas.tx.publicsearch.us",
        "date_range": {
            "start": start_dt.date().isoformat(),
            "end":   end_dt.date().isoformat(),
        },
        "total":        len(unique),
        "with_address": with_addr,
        "records":      unique,
    }

    # ── 7. Write JSON outputs ───────────────────────────────────────────────
    for out in (DASHBOARD_JSON, DATA_JSON):
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info(f"JSON → {out}")

    # ── 8. Write GHL CSV ────────────────────────────────────────────────────
    write_ghl_csv(unique, GHL_CSV)

    log.info("=" * 65)
    log.info(f"  Done.  {len(unique)} leads | {with_addr} with address")
    log.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
