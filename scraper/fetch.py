"""
Dallas County, Texas — Motivated Seller Lead Scraper
=====================================================
Target portal : https://dallas.tx.publicsearch.us  (Neumo PublicSearch platform)
Parcel data   : https://www.dallascad.org/DataProducts.aspx
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
# Dallas County Clerk Scraper — dallas.tx.publicsearch.us
# ─────────────────────────────────────────────────────────────────────────────

class ClerkScraper:
    """
    Targets the Neumo PublicSearch portal used by Dallas County Clerk.
    Navigates directly to the results URL with all params in the query string:

      https://dallas.tx.publicsearch.us/results/document/search/advanced
        ?category=OPR&dateType=R&fromDate=MM/DD/YYYY&toDate=MM/DD/YYYY
        &docTypes=LP&page=1&perPage=50
    """

    RESULTS_URL = f"{PORTAL_BASE}/results/document/search/advanced"
    PER_PAGE    = 50
    MAX_PAGES   = 40

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.records: list[dict] = []

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
            page = await ctx.new_page()

            # Load home page once to set cookies / accept terms
            log.info("Loading portal home page …")
            try:
                await page.goto(PORTAL_BASE, timeout=30_000,
                                wait_until="domcontentloaded")
                await page.wait_for_load_state("networkidle", timeout=15_000)
                for btn_text in ["Accept", "I Agree", "Continue", "OK", "Close"]:
                    try:
                        await page.click(
                            f'button:has-text("{btn_text}"), '
                            f'a:has-text("{btn_text}")',
                            timeout=3_000,
                        )
                        log.info(f"Dismissed dialog: {btn_text}")
                        break
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"Home page warning: {e}")

            for code in ALL_CODES:
                log.info(f"Searching: {code}")
                try:
                    recs = await self._search_code(page, code)
                    log.info(f"  → {len(recs)} records")
                    self.records.extend(recs)
                except Exception as exc:
                    log.error(f"  Error on {code}: {exc}")
                    traceback.print_exc()
                await asyncio.sleep(1.0)

            await browser.close()

        log.info(f"Playwright scrape complete – {len(self.records)} raw records")

    async def _search_code(self, page, code: str) -> list[dict]:
        records  = []
        from_str = self.start.strftime("%m/%d/%Y")
        to_str   = self.end.strftime("%m/%d/%Y")

        for page_num in range(1, self.MAX_PAGES + 1):
            params = {
                "category": "OPR",
                "dateType": "R",
                "fromDate": from_str,
                "toDate":   to_str,
                "docTypes": code,
                "page":     page_num,
                "perPage":  self.PER_PAGE,
            }
            url = f"{self.RESULTS_URL}?{urlencode(params)}"

            loaded = False
            for attempt in range(3):
                try:
                    await page.goto(url, timeout=30_000,
                                    wait_until="domcontentloaded")
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    loaded = True
                    break
                except PWTimeout:
                    log.warning(f"  Timeout p{page_num} attempt {attempt+1}")
                    await asyncio.sleep(3)

            if not loaded:
                break

            html      = await page.content()
            page_recs = self._parse_page(html, code, url)
            records.extend(page_recs)

            if len(page_recs) < self.PER_PAGE or self._is_empty_page(html):
                break

        return records

    def _is_empty_page(self, html: str) -> bool:
        text = BeautifulSoup(html, "lxml").get_text(" ", strip=True).lower()
        return any(p in text for p in [
            "no results", "no records found", "0 results",
            "no documents found", "no matching",
        ])

    def _parse_page(self, html: str, code: str, url: str) -> list[dict]:
        records  = []
        soup     = BeautifulSoup(html, "lxml")

        # ── Try table layout ──
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [c.get_text(strip=True).lower()
                       for c in rows[0].find_all(["th", "td"])]
            if not any(kw in " ".join(headers)
                       for kw in ["doc", "name", "date", "type", "grantor", "party"]):
                continue

            col: dict[str, int] = {}
            for i, h in enumerate(headers):
                if re.search(r"doc.?num|instr|number", h) and "doc_num" not in col:
                    col["doc_num"] = i
                if re.search(r"type", h) and "doc_type" not in col:
                    col["doc_type"] = i
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
                        "clerk_url": clerk_url or url,
                    })
                    records.append(r)
                except Exception as exc:
                    log.debug(f"Table row error: {exc}")

        # ── Try card/div layout if table found nothing ──
        if not records:
            records = self._parse_cards(soup, code, url)

        return records

    def _parse_cards(self, soup, code: str, url: str) -> list[dict]:
        records = []
        cards = (
            soup.find_all("div", class_=re.compile(r"result|document|record|row", re.I)) or
            soup.find_all("li",  class_=re.compile(r"result|document|record", re.I))
        )
        for card in cards:
            try:
                text  = card.get_text(" ", strip=True)
                link  = card.find("a", href=True)
                clerk_url = ""
                if link:
                    h = link["href"]
                    clerk_url = urljoin(PORTAL_BASE, h) if h.startswith("/") else h

                doc_match = re.search(
                    r"(?:doc(?:ument)?[\s#:]*|instr[\w]*[\s#:]*)(\d[\w\-/]+)",
                    text, re.I)
                doc_num = doc_match.group(1) if doc_match else (
                    link.get_text(strip=True) if link else "")
                if not doc_num:
                    continue

                date_match = re.search(r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\b", text)
                filed_raw  = date_match.group(1) if date_match else ""
                dt         = parse_date(filed_raw)
                filed_iso  = dt.strftime("%Y-%m-%d") if dt else filed_raw

                grantor_match = re.search(
                    r"Grantor[:\s]+([A-Z][A-Z\s,\.]+?)(?:\s{2,}|Grantee|$)",
                    text, re.I)
                owner = grantor_match.group(1).strip() if grantor_match else ""

                grantee_match = re.search(
                    r"Grantee[:\s]+([A-Z][A-Z\s,\.]+?)(?:\s{2,}|$)",
                    text, re.I)
                grantee = grantee_match.group(1).strip() if grantee_match else ""

                amt_match = re.search(r"\$[\d,]+(?:\.\d{2})?", text)
                amount    = parse_amount(amt_match.group(0)) if amt_match else None

                r = blank_record(code)
                r.update({
                    "doc_num":   doc_num.strip(),
                    "filed":     filed_iso,
                    "owner":     owner,
                    "grantee":   grantee,
                    "amount":    amount,
                    "clerk_url": clerk_url or url,
                })
                records.append(r)
            except Exception as exc:
                log.debug(f"Card parse error: {exc}")

        return records


# ─────────────────────────────────────────────────────────────────────────────
# Requests-based fallback (same portal, no JS rendering)
# ─────────────────────────────────────────────────────────────────────────────

class ClerkRequestsFallback:
    RESULTS_URL = f"{PORTAL_BASE}/results/document/search/advanced"

    def __init__(self, start: datetime, end: datetime):
        self.start   = start
        self.end     = end
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Accept":  "text/html,application/xhtml+xml,*/*",
            "Referer": PORTAL_BASE,
        })

    def fetch_all(self) -> list[dict]:
        records: list[dict] = []
        from_str = self.start.strftime("%m/%d/%Y")
        to_str   = self.end.strftime("%m/%d/%Y")
        scraper  = ClerkScraper(self.start, self.end)

        for code in ALL_CODES:
            for page_num in range(1, 41):
                params = {
                    "category": "OPR", "dateType": "R",
                    "fromDate": from_str, "toDate": to_str,
                    "docTypes": code, "page": page_num, "perPage": 50,
                }
                try:
                    resp = self.session.get(
                        self.RESULTS_URL, params=params, timeout=30)
                    resp.raise_for_status()
                    recs = scraper._parse_page(resp.text, code, resp.url)
                    records.extend(recs)
                    if len(recs) < 50 or scraper._is_empty_page(resp.text):
                        break
                    time.sleep(0.5)
                except Exception as e:
                    log.debug(f"Requests fallback {code} p{page_num}: {e}")
                    break

        return records


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

    # 2. Playwright scrape
    clerk = ClerkScraper(start_dt, end_dt)
    await clerk.run()
    records = clerk.records

    # 3. Requests fallback if Playwright got nothing
    if not records:
        log.warning("Playwright returned 0 – trying requests fallback …")
        records = ClerkRequestsFallback(start_dt, end_dt).fetch_all()
        log.info(f"Requests fallback: {len(records)} records")

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
