"""
Dallas County, Texas — Motivated Seller Lead Scraper
=====================================================
Target portal : https://dallas.tx.publicsearch.us  (Neumo PublicSearch — React SPA)
Strategy      : Intercept the portal's own background API/JSON calls using
                Playwright's network request interception.  The SPA fetches
                results from a REST endpoint; we capture that JSON directly.
Parcel data   : https://www.dallascad.org/DataProducts.aspx (bulk DBF)
Look-back     : Last 7 days
"""

import asyncio
import csv
import io
import json
import logging
import re
import sys
import time
import traceback
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

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
PORTAL_BASE   = "https://dallas.tx.publicsearch.us"
CAD_PAGE      = "https://www.dallascad.org/DataProducts.aspx"

DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("LP",       "Lis Pendens"),
    "RELLP":    ("RELLP",    "Release Lis Pendens"),
    "NOFC":     ("NOFC",     "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED",  "Tax Deed"),
    "JUD":      ("JUD",      "Judgment"),
    "CCJ":      ("CCJ",      "Certified Judgment"),
    "DRJUD":    ("DRJUD",    "Domestic Relations Judgment"),
    "LNCORPTX": ("LNCORPTX","Corp Tax Lien"),
    "LNIRS":    ("LNIRS",    "IRS Lien"),
    "LNFED":    ("LNFED",    "Federal Lien"),
    "LN":       ("LN",       "Lien"),
    "LNMECH":   ("LNMECH",  "Mechanic Lien"),
    "LNHOA":    ("LNHOA",   "HOA Lien"),
    "MEDLN":    ("MEDLN",   "Medicaid Lien"),
    "PRO":      ("PRO",      "Probate"),
    "NOC":      ("NOC",      "Notice of Commencement"),
}
ALL_CODES = list(DOC_TYPE_MAP.keys())

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
    ("New this week",    lambda r: True),
]

# ─────────────────────────────────────────────────────────────────────────────
# Paths
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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_amount(text) -> Optional[float]:
    if not text:
        return None
    clean = re.sub(r"[^0-9.]", "", str(text))
    try:
        v = float(clean)
        return v if v > 0 else None
    except ValueError:
        return None


def name_variants(full: str) -> list[str]:
    full = re.sub(r"\s+", " ", full.strip().upper())
    parts = full.split()
    if not parts:
        return [full]
    if len(parts) == 1:
        return [full]
    first, last = parts[0], parts[-1]
    return [full, f"{last} {first}", f"{last}, {first}"]


def parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y",
                "%B %d, %Y", "%b %d, %Y", "%Y%m%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            pass
    return None


def blank_record(code: str) -> dict:
    cat, lbl = DOC_TYPE_MAP.get(code, (code, code))
    return {
        "doc_num": "", "doc_type": code, "filed": "",
        "cat": cat, "cat_label": lbl,
        "owner": "", "grantee": "", "amount": None, "legal": "",
        "clerk_url": "",
        "prop_address": "", "prop_city": "Dallas",
        "prop_state": "TX", "prop_zip": "",
        "mail_address": "", "mail_city": "",
        "mail_state": "", "mail_zip": "",
        "flags": [], "score": 0,
    }


def normalize_api_record(item: dict, code: str) -> Optional[dict]:
    """Convert one item from the portal's JSON API into our record format."""
    try:
        cat, lbl = DOC_TYPE_MAP.get(code, (code, code))

        # Doc number — various field names seen in publicsearch APIs
        doc_num = str(
            item.get("instrumentNumber") or
            item.get("docNumber") or
            item.get("documentNumber") or
            item.get("id") or ""
        ).strip()
        if not doc_num:
            return None

        # Filed date
        filed_raw = str(
            item.get("recordedDate") or
            item.get("fileDate") or
            item.get("instrumentDate") or
            item.get("date") or ""
        )
        dt        = parse_date(filed_raw)
        filed_iso = dt.strftime("%Y-%m-%d") if dt else filed_raw

        # Parties
        # grantor / grantors list
        grantor_list = item.get("grantors") or item.get("grantor") or []
        if isinstance(grantor_list, list):
            owner = "; ".join(
                p.get("name", "") if isinstance(p, dict) else str(p)
                for p in grantor_list
            ).strip()
        else:
            owner = str(grantor_list).strip()

        grantee_list = item.get("grantees") or item.get("grantee") or []
        if isinstance(grantee_list, list):
            grantee = "; ".join(
                p.get("name", "") if isinstance(p, dict) else str(p)
                for p in grantee_list
            ).strip()
        else:
            grantee = str(grantee_list).strip()

        # Amount
        amount = parse_amount(
            item.get("consideration") or item.get("amount") or
            item.get("docAmount") or ""
        )

        # Legal description
        legal = str(item.get("legalDescription") or item.get("legal") or "").strip()

        # Direct document URL
        doc_id    = item.get("id") or doc_num
        clerk_url = f"{PORTAL_BASE}/doc/{doc_id}" if doc_id else ""

        r = blank_record(code)
        r.update({
            "doc_num":   doc_num,
            "filed":     filed_iso,
            "owner":     owner,
            "grantee":   grantee,
            "amount":    amount,
            "legal":     legal,
            "clerk_url": clerk_url,
        })
        return r
    except Exception as exc:
        log.debug(f"normalize_api_record error: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Dallas CAD Parcel Lookup
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._index: dict[str, dict] = {}

    def build(self):
        log.info("Fetching Dallas CAD parcel data …")
        dbf_bytes = self._download()
        if dbf_bytes:
            self._parse(dbf_bytes)
            log.info(f"Parcel index: {len(self._index):,} entries")
        else:
            log.warning("Parcel data unavailable – skipping address enrichment")

    def lookup(self, owner: str) -> dict:
        if not owner:
            return {}
        for v in name_variants(owner):
            hit = self._index.get(v)
            if hit:
                return hit
        return {}

    def _download(self) -> Optional[bytes]:
        session = requests.Session()
        session.headers["User-Agent"] = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        )
        try:
            resp = session.get(CAD_PAGE, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            log.warning(f"CAD page error: {e}")
            return None

        soup = BeautifulSoup(resp.text, "lxml")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(href.lower().endswith(ext) for ext in (".zip", ".dbf", ".csv")):
                url = urljoin(CAD_PAGE, href)
                try:
                    dl = session.get(url, timeout=120)
                    dl.raise_for_status()
                    result = self._extract_dbf(dl.content)
                    if result:
                        return result
                except Exception as e:
                    log.debug(f"Direct CAD link failed: {e}")

        vs  = (soup.find("input", {"id": "__VIEWSTATE"})          or {}).get("value", "")
        ev  = (soup.find("input", {"id": "__EVENTVALIDATION"})    or {}).get("value", "")
        vsg = (soup.find("input", {"id": "__VIEWSTATEGENERATOR"}) or {}).get("value", "")

        for target in [
            "lnkResidential", "lnkCommercial", "lnkAllParcels",
            "ctl00$ContentPlaceHolder1$lnkResidential",
            "ctl00$ContentPlaceHolder1$lnkCommercial",
        ]:
            try:
                dl = session.post(CAD_PAGE, timeout=120, data={
                    "__VIEWSTATE": vs, "__EVENTVALIDATION": ev,
                    "__VIEWSTATEGENERATOR": vsg,
                    "__EVENTTARGET": target, "__EVENTARGUMENT": "",
                })
                if dl.ok and len(dl.content) > 2000:
                    result = self._extract_dbf(dl.content)
                    if result:
                        return result
            except Exception:
                pass

        log.warning("Could not obtain CAD parcel file via any method.")
        return None

    def _extract_dbf(self, content: bytes) -> Optional[bytes]:
        if content[:2] == b"PK":
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".dbf"):
                            return zf.read(name)
            except Exception:
                pass
            return None
        if content[:1] in (b"\x03", b"\x83", b"\xf5"):
            return content
        return None

    def _parse(self, dbf_bytes: bytes):
        if not HAS_DBF:
            return
        tmp = Path("/tmp/_parcels.dbf")
        tmp.write_bytes(dbf_bytes)
        try:
            tbl = DBF(str(tmp), lowernames=True,
                      ignore_missing_memofile=True, encoding="latin-1")
            for row in tbl:
                try:
                    owner = str(row.get("owner") or row.get("own1") or "").strip().upper()
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
                    for v in name_variants(owner):
                        self._index.setdefault(v, parcel)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"DBF parse error: {e}")
        finally:
            tmp.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Dallas County Clerk Scraper — Network Interception Strategy
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Uses Playwright to load the PublicSearch SPA and intercepts the
    background API/JSON calls the app makes to fetch search results.

    The Neumo platform typically calls one of these endpoints:
      /api/search   (GET with query params)
      /api/documents/search  (POST with JSON body)
      /results/...  (navigated URL that triggers an XHR fetch)

    We capture ALL JSON responses from the portal domain and parse them.
    """

    PER_PAGE  = 50
    MAX_PAGES = 40

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.records: list[dict] = []
        # Captured API responses keyed by (code, page)
        self._api_cache: dict[str, list[dict]] = {}

    async def run(self):
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )

            # ── Intercept all JSON responses ──────────────────────────────
            captured: list[dict] = []

            async def handle_response(response):
                try:
                    url = response.url
                    ct  = response.headers.get("content-type", "")
                    if "json" in ct and PORTAL_BASE in url:
                        body = await response.json()
                        captured.append({"url": url, "body": body})
                        log.debug(f"Captured JSON from: {url}")
                except Exception:
                    pass

            page = await ctx.new_page()
            page.on("response", handle_response)

            # Load home page first to get cookies
            log.info("Loading portal home page …")
            try:
                await page.goto(PORTAL_BASE, timeout=30_000,
                                wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=15_000)
                for btn in ["Accept", "I Agree", "Continue", "OK", "Close"]:
                    try:
                        await page.click(
                            f'button:has-text("{btn}"), a:has-text("{btn}")',
                            timeout=3_000)
                        break
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"Home page warning: {e}")

            from_str = self.start.strftime("%m/%d/%Y")
            to_str   = self.end.strftime("%m/%d/%Y")

            for code in ALL_CODES:
                log.info(f"Searching: {code}")
                code_records: list[dict] = []
                captured.clear()

                for page_num in range(1, self.MAX_PAGES + 1):
                    captured.clear()
                    params = {
                        "category": "OPR",
                        "dateType": "R",
                        "fromDate": from_str,
                        "toDate":   to_str,
                        "docTypes": code,
                        "page":     page_num,
                        "perPage":  self.PER_PAGE,
                    }
                    url = f"{PORTAL_BASE}/results/document/search/advanced?{urlencode(params)}"

                    try:
                        await page.goto(url, timeout=30_000,
                                        wait_until="domcontentloaded")
                        # Wait for network to settle so API calls complete
                        await page.wait_for_load_state("networkidle", timeout=20_000)
                        # Extra wait for JS rendering
                        await asyncio.sleep(2)
                    except Exception as e:
                        log.warning(f"  Page load error p{page_num}: {e}")
                        break

                    # ── Parse intercepted JSON responses ──────────────────
                    page_items = []
                    for cap in captured:
                        items = self._extract_items_from_json(cap["body"])
                        page_items.extend(items)

                    if not page_items:
                        # Fallback: try to parse rendered HTML
                        html       = await page.content()
                        page_items = self._parse_html_fallback(html, code)

                    # Normalize to our record format
                    for item in page_items:
                        if isinstance(item, dict) and "doc_num" in item:
                            # Already normalized (from HTML parser)
                            code_records.append(item)
                        else:
                            r = normalize_api_record(item, code)
                            if r:
                                code_records.append(r)

                    log.debug(f"  p{page_num}: {len(page_items)} items")

                    # Stop paginating if we got less than a full page
                    if len(page_items) < self.PER_PAGE:
                        break

                log.info(f"  → {len(code_records)} records")
                self.records.extend(code_records)
                await asyncio.sleep(1.0)

            await browser.close()

        log.info(f"Playwright scrape complete – {len(self.records)} raw records")

    def _extract_items_from_json(self, body) -> list:
        """
        Extract document items from whatever JSON structure the portal returns.
        Handles: list, {results:[]}, {data:[]}, {documents:[]}, {hits:[]}, etc.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("results", "data", "documents", "hits", "records",
                        "items", "content", "rows"):
                val = body.get(key)
                if isinstance(val, list) and val:
                    return val
            # Nested under "search" or similar
            for key in ("search", "response", "payload"):
                val = body.get(key)
                if isinstance(val, dict):
                    for inner in ("results", "data", "documents", "hits"):
                        v2 = val.get(inner)
                        if isinstance(v2, list) and v2:
                            return v2
        return []

    def _parse_html_fallback(self, html: str, code: str) -> list[dict]:
        """Parse rendered HTML results if JSON interception yielded nothing."""
        records = []
        soup    = BeautifulSoup(html, "lxml")

        # Table layout
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [c.get_text(strip=True).lower()
                       for c in rows[0].find_all(["th", "td"])]
            if not any(kw in " ".join(headers)
                       for kw in ["doc", "name", "date", "grantor", "party"]):
                continue

            col: dict[str, int] = {}
            for i, h in enumerate(headers):
                if re.search(r"doc.?num|instr|number", h) and "doc_num" not in col:
                    col["doc_num"] = i
                if re.search(r"date|filed|record", h) and "filed" not in col:
                    col["filed"] = i
                if re.search(r"grantor|name|party", h) and "grantor" not in col:
                    col["grantor"] = i
                if re.search(r"grantee", h) and "grantee" not in col:
                    col["grantee"] = i
                if re.search(r"legal|desc", h) and "legal" not in col:
                    col["legal"] = i
                if re.search(r"amount|consid", h) and "amount" not in col:
                    col["amount"] = i

            for row in rows[1:]:
                cells = row.find_all(["td", "th"])
                if not cells:
                    continue
                try:
                    def ct(key):
                        idx = col.get(key)
                        return cells[idx].get_text(strip=True) \
                            if (idx is not None and idx < len(cells)) else ""
                    link_tag  = row.find("a", href=True)
                    clerk_url = ""
                    if link_tag:
                        h = link_tag["href"]
                        clerk_url = urljoin(PORTAL_BASE, h) \
                            if h.startswith("/") else h
                    doc_num = ct("doc_num") or \
                        (link_tag.get_text(strip=True) if link_tag else "")
                    if not doc_num:
                        continue
                    filed_raw = ct("filed")
                    dt        = parse_date(filed_raw)
                    filed_iso = dt.strftime("%Y-%m-%d") if dt else filed_raw
                    r = blank_record(code)
                    r.update({
                        "doc_num":   doc_num.strip(),
                        "filed":     filed_iso,
                        "owner":     ct("grantor").strip(),
                        "grantee":   ct("grantee").strip(),
                        "amount":    parse_amount(ct("amount")),
                        "legal":     ct("legal").strip(),
                        "clerk_url": clerk_url,
                    })
                    records.append(r)
                except Exception:
                    pass

        return records


# ─────────────────────────────────────────────────────────────────────────────
# Direct API Scraper (no browser)
# ─────────────────────────────────────────────────────────────────────────────

class DirectAPIScraper:
    """
    Tries to call the portal's backend REST API directly with requests.
    The Neumo platform uses endpoints like:
      GET  /api/search?...
      POST /api/documents/search
    Discovered by inspecting similar Neumo deployments.
    """

    CANDIDATE_ENDPOINTS = [
        # GET endpoints
        ("{base}/api/search",                    "GET"),
        ("{base}/api/documents",                 "GET"),
        ("{base}/api/records/search",            "GET"),
        # POST endpoints
        ("{base}/api/documents/search",          "POST"),
        ("{base}/api/search/advanced",           "POST"),
    ]

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept":       "application/json, text/plain, */*",
            "Referer":      PORTAL_BASE,
            "Origin":       PORTAL_BASE,
            "Content-Type": "application/json",
        })
        # Seed cookies by hitting the homepage first
        try:
            self.session.get(PORTAL_BASE, timeout=15)
        except Exception:
            pass

    def fetch_all(self) -> list[dict]:
        records: list[dict] = []
        from_str = self.start.strftime("%m/%d/%Y")
        to_str   = self.end.strftime("%m/%d/%Y")

        for code in ALL_CODES:
            for page_num in range(1, 41):
                items = self._try_endpoints(code, from_str, to_str, page_num)
                if items is None:
                    break  # No endpoint worked
                for item in items:
                    r = normalize_api_record(item, code)
                    if r:
                        records.append(r)
                if len(items) < 50:
                    break
                time.sleep(0.3)

        return records

    def _try_endpoints(self, code, from_str, to_str, page_num) -> Optional[list]:
        params_get = {
            "category": "OPR", "dateType": "R",
            "fromDate": from_str, "toDate": to_str,
            "docTypes": code, "page": page_num, "perPage": 50,
        }
        body_post = {
            "category": "OPR", "dateType": "R",
            "fromDate": from_str, "toDate": to_str,
            "docTypes": [code], "page": page_num, "perPage": 50,
        }

        for tmpl, method in self.CANDIDATE_ENDPOINTS:
            url = tmpl.format(base=PORTAL_BASE)
            try:
                if method == "GET":
                    r = self.session.get(url, params=params_get, timeout=20)
                else:
                    r = self.session.post(url, json=body_post, timeout=20)

                if r.status_code in (401, 403, 404):
                    continue
                r.raise_for_status()
                if "json" not in r.headers.get("content-type", ""):
                    continue

                data  = r.json()
                items = self._extract(data)
                if items is not None:
                    log.debug(f"Direct API hit: {method} {url} → {len(items)} items")
                    return items
            except Exception as e:
                log.debug(f"Direct API {url}: {e}")

        return None

    def _extract(self, body) -> Optional[list]:
        if isinstance(body, list) and body:
            return body
        if isinstance(body, dict):
            for key in ("results", "data", "documents", "hits",
                        "records", "items", "content"):
                val = body.get(key)
                if isinstance(val, list):
                    return val
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def score_record(record: dict, all_records: list[dict]) -> dict:
    flags: list[str] = []
    for label, pred in FLAG_DEFS:
        try:
            if pred(record):
                flags.append(label)
        except Exception:
            pass

    score = 30 + 10 * len(flags)

    owner = (record.get("owner") or "").strip().upper()
    if owner:
        sibling_cats = {r["cat"] for r in all_records
                        if (r.get("owner") or "").strip().upper() == owner}
        if "LP" in sibling_cats and sibling_cats & {"NOFC", "TAXDEED"}:
            score += 20

    amt = record.get("amount") or 0
    if amt > 100_000:
        score += 15
    elif amt > 50_000:
        score += 10

    if record.get("prop_address") or record.get("mail_address"):
        score += 5

    record["flags"] = flags
    record["score"] = min(int(score), 100)
    return record


# ─────────────────────────────────────────────────────────────────────────────
# GHL CSV Export
# ─────────────────────────────────────────────────────────────────────────────

GHL_FIELDS = [
    "First Name", "Last Name",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]


def write_ghl_csv(records: list[dict], path: Path):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=GHL_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in records:
            parts = re.split(r"\s+", (r.get("owner") or "").strip())
            first = parts[0].title() if parts else ""
            last  = " ".join(parts[1:]).title() if len(parts) > 1 else ""
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
    log.info("  Dallas County TX — Motivated Seller Lead Scraper")
    log.info(f"  Portal  : {PORTAL_BASE}")
    log.info(f"  Range   : {start_dt.date()} → {end_dt.date()}")
    log.info("=" * 65)

    # 1. Parcel index
    parcel = ParcelLookup()
    parcel.build()

    # 2. Try direct API first (fast, no browser needed)
    log.info("Trying direct API endpoints …")
    records = DirectAPIScraper(start_dt, end_dt).fetch_all()
    log.info(f"Direct API: {len(records)} records")

    # 3. Playwright with network interception if direct API got nothing
    if not records:
        log.info("Falling back to Playwright network interception …")
        clerk = ClerkScraper(start_dt, end_dt)
        await clerk.run()
        records = clerk.records

    log.info(f"Raw total : {len(records)}")

    # 4. Deduplicate
    seen: set[str] = set()
    unique: list[dict] = []
    for r in records:
        key = f"{r.get('doc_num','').strip()}|{r.get('doc_type','')}"
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    log.info(f"After dedup: {len(unique)}")

    # 5. Parcel enrichment
    with_addr = 0
    for r in unique:
        hit = parcel.lookup(r.get("owner", ""))
        if hit:
            r.update(hit)
            with_addr += 1
    log.info(f"With address: {with_addr}")

    # 6. Score
    for r in unique:
        score_record(r, unique)
    unique.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 7. Write JSON
    payload = {
        "fetched_at": now.isoformat(),
        "source":     "Dallas County Clerk (dallas.tx.publicsearch.us)",
        "date_range": {
            "start": start_dt.date().isoformat(),
            "end":   end_dt.date().isoformat(),
        },
        "total":        len(unique),
        "with_address": with_addr,
        "records":      unique,
    }
    for out in (DASHBOARD_JSON, DATA_JSON):
        out.write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info(f"JSON → {out}")

    # 8. GHL CSV
    write_ghl_csv(unique, GHL_CSV)

    log.info("=" * 65)
    log.info(f"  Done. {len(unique)} leads | {with_addr} with address")
    log.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
