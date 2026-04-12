"""
Dallas County, Texas — Motivated Seller Lead Scraper
=====================================================
Target portal : https://dallas.tx.publicsearch.us
Proxy         : Decodo residential rotating proxy
Parcel data   : https://www.dallascad.org/DataProducts.aspx
Look-back     : Last 7 days
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
# Decodo proxy config
# ─────────────────────────────────────────────────────────────────────────────
DECODO_USER = os.environ.get("DECODO_USER", "")
DECODO_PASS = os.environ.get("DECODO_PASS", "")
DECODO_HOST = "gate.decodo.com"
DECODO_PORT = 7000

PROXY_URL_AUTH = f"http://{DECODO_USER}:{DECODO_PASS}@{DECODO_HOST}:{DECODO_PORT}"
PROXY_URL_BARE = f"http://{DECODO_HOST}:{DECODO_PORT}"

def proxy_enabled() -> bool:
    return bool(DECODO_USER and DECODO_PASS)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS = 7
PORTAL_BASE   = "http://dallas.tx.publicsearch.us"
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


def make_requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    if proxy_enabled():
        session.proxies = {
            "http":  PROXY_URL_AUTH,
            "https": PROXY_URL_AUTH,
        }
        log.info(f"Requests session using Decodo proxy: {DECODO_HOST}:{DECODO_PORT}")
    return session


# ─────────────────────────────────────────────────────────────────────────────
# Dallas CAD Parcel Lookup
# ─────────────────────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._index: dict[str, dict] = {}

    def build(self):
        log.info("Fetching Dallas CAD parcel data …")
        if proxy_enabled():
            log.info("Skipping CAD download (proxy mode)")
            log.warning("Parcel data unavailable – skipping address enrichment")
            return
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
        session = make_requests_session()
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
# Dallas County Clerk Scraper
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Playwright scraper routed through Decodo residential proxy.
    The portal is a React SPA — results are embedded as JSON in the page.
    We extract JSON blobs containing 'docNumber' directly from page source.
    """

    PER_PAGE  = 50
    MAX_PAGES = 40

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.records: list[dict] = []

    async def run(self):
        async with async_playwright() as pw:
            launch_args = ["--no-sandbox", "--disable-dev-shm-usage"]
            context_kwargs = {
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                ),
                "viewport":           {"width": 1280, "height": 900},
                "locale":             "en-US",
                "timezone_id":        "America/Chicago",
                "extra_http_headers": {"Accept-Language": "en-US,en;q=0.9"},
            }

            if proxy_enabled():
                log.info(f"Playwright using Decodo proxy: {DECODO_HOST}:{DECODO_PORT}")
                context_kwargs["proxy"] = {
                    "server":   PROXY_URL_BARE,
                    "username": DECODO_USER,
                    "password": DECODO_PASS,
                }
            else:
                log.warning("No proxy configured")

            browser = await pw.chromium.launch(headless=True, args=launch_args)
            ctx     = await browser.new_context(**context_kwargs)
            page    = await ctx.new_page()

            # Capture JSON API responses
            captured: list[dict] = []

            async def handle_response(response):
                try:
                    url = response.url
                    ct  = response.headers.get("content-type", "")
                    if "json" in ct and PORTAL_BASE in url:
                        body = await response.json()
                        captured.append({"url": url, "body": body})
                except Exception:
                    pass

            page.on("response", handle_response)

            # Load home page
            log.info("Loading portal home page …")
            try:
                await page.goto(PORTAL_BASE, timeout=60_000,
                                wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=20_000)
                for btn in ["Accept", "I Agree", "Continue", "OK", "Close"]:
                    try:
                        await page.click(
                            f'button:has-text("{btn}"), a:has-text("{btn}")',
                            timeout=2_000)
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

                    loaded = False
                    for attempt in range(3):
                        try:
                            await page.goto(url, timeout=60_000,
                                            wait_until="domcontentloaded")
                            await page.wait_for_load_state("networkidle",
                                                           timeout=25_000)
                            await asyncio.sleep(8)
                            loaded = True
                            break
                        except PWTimeout:
                            log.warning(f"  Timeout p{page_num} attempt {attempt+1}")
                            await asyncio.sleep(4)

                    if not loaded:
                        break

                    html = await page.content()
                    log.info(f"  Page length: {len(html)} chars")

                    # Try intercepted JSON first
                    page_items: list[dict] = []
                    for cap in captured:
                        items = self._extract_items(cap["body"])
                        page_items.extend(items)

                    # Fallback: extract JSON blobs from page source
                    if not page_items:
                        page_items = self._extract_json_from_html(html, code)

                    log.info(f"  Items found: {len(page_items)}")
                    log.info(f"  HTML sample: {html[40000:40300]}")
                    for item in page_items:
                        if isinstance(item, dict) and "doc_num" in item:
                            code_records.append(item)
                        else:
                            r = self._normalize(item, code)
                            if r:
                                code_records.append(r)

                    if len(page_items) < self.PER_PAGE:
                        break

                log.info(f"  → {len(code_records)} records")
                self.records.extend(code_records)
                await asyncio.sleep(1.0)

            await browser.close()

        log.info(f"Playwright complete – {len(self.records)} raw records")

    def _extract_items(self, body) -> list:
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("results", "data", "documents", "hits",
                        "records", "items", "content", "rows"):
                val = body.get(key)
                if isinstance(val, list) and val:
                    return val
            for key in ("search", "response", "payload"):
                val = body.get(key)
                if isinstance(val, dict):
                    for inner in ("results", "data", "documents", "hits"):
                        v2 = val.get(inner)
                        if isinstance(v2, list) and v2:
                            return v2
        return []

    def _extract_json_from_html(self, html: str, code: str) -> list[dict]:
        """
        The React app embeds search results as JSON in the page source.
        We find all JSON objects containing 'docNumber' and parse them.
        Also tries to find larger JSON arrays containing result sets.
        """
        records = []

        # Strategy 1: find individual doc objects
        # Look for JSON objects with docNumber field
        pattern = re.compile(r'\{[^{}]{0,2000}"docNumber"[^{}]{0,2000}\}', re.DOTALL)
        for match in pattern.finditer(html):
            try:
                item = json.loads(match.group())
                r = self._normalize(item, code)
                if r:
                    records.append(r)
            except Exception:
                pass

        # Strategy 2: find larger JSON arrays with results
        if not records:
            # Look for arrays that contain objects with docNumber
            array_pattern = re.compile(r'\[(\{[^[\]]*"docNumber"[^[\]]*\}[,\s]*)+\]', re.DOTALL)
            for match in array_pattern.finditer(html):
                try:
                    items = json.loads(match.group())
                    for item in items:
                        r = self._normalize(item, code)
                        if r:
                            records.append(r)
                except Exception:
                    pass

        # Strategy 3: extract from __NEXT_DATA__ or window.__STATE__ etc
        if not records:
            state_pattern = re.compile(
                r'(?:__NEXT_DATA__|window\.__STATE__|window\.__data__)[\s]*=[\s]*(\{.+?\});',
                re.DOTALL)
            for match in state_pattern.finditer(html):
                try:
                    data = json.loads(match.group(1))
                    items = self._deep_find_docs(data)
                    for item in items:
                        r = self._normalize(item, code)
                        if r:
                            records.append(r)
                except Exception:
                    pass

        return records

    def _deep_find_docs(self, obj, depth=0) -> list:
        """Recursively search a JSON structure for objects with docNumber."""
        if depth > 8:
            return []
        results = []
        if isinstance(obj, dict):
            if "docNumber" in obj or "instrumentNumber" in obj:
                results.append(obj)
            else:
                for v in obj.values():
                    results.extend(self._deep_find_docs(v, depth + 1))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(self._deep_find_docs(item, depth + 1))
        return results

    def _normalize(self, item: dict, code: str) -> Optional[dict]:
        try:
            doc_num = str(
                item.get("docNumber") or item.get("instrumentNumber") or
                item.get("documentNumber") or item.get("id") or ""
            ).strip()
            if not doc_num:
                return None

            filed_raw = str(
                item.get("recordedDate") or item.get("fileDate") or
                item.get("instrumentDate") or item.get("date") or ""
            )
            dt        = parse_date(filed_raw)
            filed_iso = dt.strftime("%Y-%m-%d") if dt else filed_raw

            # Grantor / owner
            grantor = item.get("grantors") or item.get("grantor") or \
                      item.get("grantorName") or ""
            if isinstance(grantor, list):
                owner = "; ".join(
                    p.get("name", "") if isinstance(p, dict) else str(p)
                    for p in grantor).strip()
            else:
                owner = str(grantor).strip()

            # Grantee
            grantee_raw = item.get("grantees") or item.get("grantee") or \
                          item.get("granteeName") or ""
            if isinstance(grantee_raw, list):
                grantee = "; ".join(
                    p.get("name", "") if isinstance(p, dict) else str(p)
                    for p in grantee_raw).strip()
            else:
                grantee = str(grantee_raw).strip()

            amount    = parse_amount(
                item.get("consideration") or item.get("amount") or
                item.get("docAmount") or "")
            legal     = str(item.get("legalDescription") or
                            item.get("legal") or "").strip()
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
            log.debug(f"normalize error: {exc}")
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
    log.info(f"  Proxy   : {'Decodo ENABLED' if proxy_enabled() else 'DISABLED'}")
    log.info("=" * 65)

    # Proxy test
    if proxy_enabled():
        try:
            test = requests.get(
                "https://ip.decodo.com/json",
                proxies={"http": PROXY_URL_AUTH, "https": PROXY_URL_AUTH},
                timeout=30)
            log.info(f"Proxy test: {test.text[:120]}")
        except Exception as e:
            log.warning(f"Proxy test failed: {e}")

    # 1. Parcel index
    parcel = ParcelLookup()
    parcel.build()

    # 2. Playwright scrape
    clerk = ClerkScraper(start_dt, end_dt)
    await clerk.run()
    records = clerk.records

    log.info(f"Raw total : {len(records)}")

    # 3. Deduplicate
    seen: set[str] = set()
    unique: list[dict] = []
    for r in records:
        key = f"{r.get('doc_num','').strip()}|{r.get('doc_type','')}"
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    log.info(f"After dedup: {len(unique)}")

    # 4. Parcel enrichment
    with_addr = 0
    for r in unique:
        hit = parcel.lookup(r.get("owner", ""))
        if hit:
            r.update(hit)
            with_addr += 1
    log.info(f"With address: {with_addr}")

    # 5. Score
    for r in unique:
        score_record(r, unique)
    unique.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 6. Write JSON
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

    # 7. GHL CSV
    write_ghl_csv(unique, GHL_CSV)

    log.info("=" * 65)
    log.info(f"  Done. {len(unique)} leads | {with_addr} with address")
    log.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
