"""
Dallas County, Texas — Motivated Seller Lead Scraper
=====================================================
Source  : Dallas Central Appraisal District (DCAD)
          https://www.dallascad.org/DataProducts.aspx
Data    : 2026 Current Ownership + Residential Property Data
"""

import csv
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
log = logging.getLogger("dcad_scraper")

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
CAD_BASE = "https://www.dallascad.org"
CAD_PAGE = "https://www.dallascad.org/DataProducts.aspx"
CAD_2026_URL = (
    "https://www.dallascad.org/ViewPDFs.aspx?type=3&id="
    "\\\\DCAD.ORG\\WEB\\WEBDATA\\WEBFORMS\\DATA PRODUCTS\\DCAD2026_CURRENT.ZIP"
)

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR      = ROOT / "data"
CACHE_DIR     = ROOT / "data" / "cache"
for d in (DASHBOARD_DIR, DATA_DIR, CACHE_DIR):
    d.mkdir(parents=True, exist_ok=True)

DASHBOARD_JSON = DASHBOARD_DIR / "records.json"
DATA_JSON      = DATA_DIR      / "records.json"
GHL_CSV        = DATA_DIR      / "ghl_export.csv"
CACHE_ZIP      = CACHE_DIR     / "dcad2026_current.zip"

# ─────────────────────────────────────────────────────────────────────────────
# Flag definitions: (label, score_bonus, condition_fn)
# ─────────────────────────────────────────────────────────────────────────────
FLAG_DEFS = [
    ("Tax delinquent",        25, lambda r: r.get("_tax_delinquent", False)),
    ("Probate / estate",      20, lambda r: bool(re.search(
        r'\b(ESTATE|DECEASED|HEIRS?|DEVISEE)\b',
        (r.get("owner") or ""), re.I))),
    ("Absentee owner",        15, lambda r: _is_absentee(r)),
    ("LLC / corp owner",      15, lambda r: bool(re.search(
        r'\b(LLC|INC|CORP|LP\b|LTD|TRUST|HOLDING|INVESTMENT|PROPERTIES|VENTURES)\b',
        (r.get("owner") or ""), re.I))),
    ("Homestead removed",     15, lambda r: r.get("_homestead_removed", False)),
    ("Vacant / no homestead", 10, lambda r: (
        not r.get("_has_homestead", False) and
        (r.get("_appraised_value") or 0) > 100_000)),
    ("Out of state owner",    10, lambda r: (
        r.get("mail_state", "").upper() not in ("TX", "TEXAS", "", None))),
    ("High value property",    5, lambda r: (r.get("_appraised_value") or 0) > 300_000),
    ("Value drop",            10, lambda r: r.get("_value_dropped", False)),
]


def _is_absentee(r: dict) -> bool:
    prop_city = (r.get("prop_city") or "").upper().strip()
    mail_city = (r.get("mail_city") or "").upper().strip()
    if mail_city and prop_city and mail_city != prop_city:
        return True
    if r.get("mail_state", "").upper() not in ("TX", "TEXAS", ""):
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(val) -> Optional[float]:
    try:
        v = float(str(val).replace(",", "").strip())
        return v if v > 0 else None
    except (ValueError, TypeError):
        return None


def g(row: dict, *keys) -> str:
    for k in keys:
        for variant in (k, k.upper(), k.lower()):
            v = row.get(variant)
            if v:
                s = str(v).strip()
                if s:
                    return s
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# Downloader
# ─────────────────────────────────────────────────────────────────────────────

class DCADDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
            "Referer": CAD_PAGE,
        })

    def get_zip(self) -> Optional[bytes]:
        today      = datetime.now().strftime("%Y-%m-%d")
        stamp_file = CACHE_DIR / "download_date.txt"

        if CACHE_ZIP.exists() and stamp_file.exists():
            if stamp_file.read_text().strip() == today:
                log.info(f"Using cached DCAD data from {today}")
                return CACHE_ZIP.read_bytes()

        log.info("Downloading DCAD 2026 current data …")

        for url in self._get_urls():
            log.info(f"Trying: {url[:80]} …")
            for attempt in range(3):
                try:
                    resp = self.session.get(url, timeout=180, stream=True)
                    if resp.status_code == 200:
                        content = resp.content
                        if len(content) > 10_000 and content[:2] == b"PK":
                            log.info(f"Downloaded {len(content):,} bytes")
                            CACHE_ZIP.write_bytes(content)
                            stamp_file.write_text(today)
                            return content
                    log.warning(f"  Status {resp.status_code} (attempt {attempt+1})")
                except Exception as e:
                    log.warning(f"  Error (attempt {attempt+1}): {e}")
                time.sleep(5)

        log.error("Could not download DCAD data")
        return None

    def _get_urls(self) -> list[str]:
        urls = []
        try:
            resp = self.session.get(CAD_PAGE, timeout=30)
            soup = BeautifulSoup(resp.text, "lxml")
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True)
                href = a["href"]
                if "2026" in text and "current" in text.lower() and "ownership" in text.lower():
                    urls.append(urljoin(CAD_BASE, href))
                    break
        except Exception as e:
            log.warning(f"Page parse error: {e}")

        urls.append(CAD_2026_URL)
        urls.append(
            "https://www.dallascad.org/webforms/DATA%20PRODUCTS/DCAD2026_CURRENT.ZIP"
        )
        return urls


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────

class DCADParser:
    RESIDENTIAL_CODES = {"A", "B", "C", "D", "E"}

    def __init__(self, zip_bytes: bytes):
        self.zip_bytes        = zip_bytes
        self.records:         list[dict]      = []
        self._res_accounts:   set[str]        = set()
        self._values:         dict[str, dict] = {}
        self._exemptions:     dict[str, dict] = {}
        self._tax_delinquent: set[str]        = set()
        self._res_details:    dict[str, dict] = {}

    def parse(self) -> list[dict]:
        log.info("Parsing DCAD ZIP …")
        try:
            with zipfile.ZipFile(io.BytesIO(self.zip_bytes)) as zf:
                names = [n.lower() for n in zf.namelist()]
                log.info(f"ZIP contains {len(names)} files: {names[:10]}")

                # Show columns for debugging
                rows_sample = self._get_file(zf, "account_info")
                if rows_sample:
                    log.info(f"account_info columns: {list(rows_sample[0].keys())[:30]}")

                self._load_residential_accounts(zf)
                self._load_values(zf)
                self._load_exemptions(zf)
                self._load_tax_status(zf)
                self._load_res_details(zf)
                self._parse_accounts(zf)

        except Exception as e:
            log.error(f"ZIP parse error: {e}")
            import traceback
            traceback.print_exc()

        log.info(f"Parsed {len(self.records):,} residential records")
        return self.records

    def _get_file(self, zf: zipfile.ZipFile, *patterns) -> Optional[list[dict]]:
        for pattern in patterns:
            for zname in zf.namelist():
                if pattern.lower() in zname.lower():
                    try:
                        content = zf.read(zname)
                        if zname.lower().endswith(".csv"):
                            return self._parse_csv(content)
                        if zname.lower().endswith(".dbf") and HAS_DBF:
                            return self._parse_dbf(content, zname)
                        return self._parse_csv(content)
                    except Exception as e:
                        log.debug(f"Could not read {zname}: {e}")
        return None

    def _parse_csv(self, content: bytes) -> list[dict]:
        rows = []
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                text   = content.decode(encoding)
                reader = csv.DictReader(io.StringIO(text))
                for row in reader:
                    rows.append({k.strip().upper(): (v or "").strip()
                                  for k, v in row.items() if k})
                return rows
            except Exception:
                pass
        return rows

    def _parse_dbf(self, content: bytes, name: str) -> list[dict]:
        tmp  = Path(f"/tmp/_dcad_{name.split('/')[-1]}")
        rows = []
        tmp.write_bytes(content)
        try:
            tbl = DBF(str(tmp), lowernames=False,
                      ignore_missing_memofile=True, encoding="latin-1")
            for row in tbl:
                rows.append({k.strip().upper(): str(v).strip() if v else ""
                              for k, v in row.items()})
        except Exception as e:
            log.debug(f"DBF error: {e}")
        finally:
            tmp.unlink(missing_ok=True)
        return rows

    def _load_residential_accounts(self, zf):
        rows = self._get_file(zf, "residential", "res_")
        if rows:
            for row in rows:
                acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
                if acct:
                    self._res_accounts.add(acct)
            log.info(f"Residential accounts: {len(self._res_accounts):,}")

        rows2 = self._get_file(zf, "account_info", "account")
        if rows2:
            for row in rows2:
                state_cd = g(row, "STATE_CD", "STATECODE", "PROP_TYPE",
                             "PROPERTY_TYPE", "CAT_CD", "CATEGORY", "SPTD_CODE")
                acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
                if acct and state_cd and state_cd.upper()[:1] in self.RESIDENTIAL_CODES:
                    self._res_accounts.add(acct)

    def _load_values(self, zf):
        rows = self._get_file(zf, "value", "appval", "appr_val", "account_apprl")
        if not rows:
            return
        for row in rows:
            acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
            if acct:
                self._values[acct] = {
                    "appraised": safe_float(g(row, "APPRAISED_VAL", "APPR_VAL",
                                              "TOTAL_VAL", "MARKET_VAL", "MKT_VAL",
                                              "APPRAISED", "TOT_VAL")),
                    "land":      safe_float(g(row, "LAND_VAL", "LAND_VALUE", "LAND")),
                    "impr":      safe_float(g(row, "IMPR_VAL", "IMPROVEMENT_VAL",
                                              "IMP_VAL", "IMPR")),
                    "prior_val": safe_float(g(row, "PRIOR_VAL", "PREV_VAL", "LAST_VAL")),
                }
        log.info(f"Value records: {len(self._values):,}")

    def _load_exemptions(self, zf):
        rows = self._get_file(zf, "applied_std_exempt", "exemption", "exempt",
                              "acct_exempt")
        if not rows:
            return
        for row in rows:
            acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
            ex   = g(row, "EXEMPTION_CD", "EXEMPT_CD", "EX_CD", "CODE",
                     "EXEMPT_TYPE", "EXEMPTION_TYPE").upper()
            if acct:
                if acct not in self._exemptions:
                    self._exemptions[acct] = {"codes": set(), "has_homestead": False}
                self._exemptions[acct]["codes"].add(ex)
                if ex in ("HS", "OV65", "DP", "HOMESTEAD", "OV65S"):
                    self._exemptions[acct]["has_homestead"] = True
        log.info(f"Exemption records: {len(self._exemptions):,}")

    def _load_tax_status(self, zf):
        rows = self._get_file(zf, "taxunit", "tax_unit", "taxdue", "delinq",
                              "account_tif")
        if not rows:
            return
        for row in rows:
            acct   = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
            delinq = g(row, "DELINQUENT", "DELINQ", "DELINQ_YR",
                       "PRIOR_YR_TAX", "PRIOR_YR").upper()
            status = g(row, "STATUS", "TAX_STATUS", "SUIT_STATUS").upper()
            if acct and (
                delinq not in ("", "0", "N", "NO", "NONE") or
                "DELIN" in status or "SUIT" in status
            ):
                self._tax_delinquent.add(acct)
        log.info(f"Tax delinquent accounts: {len(self._tax_delinquent):,}")

    def _load_res_details(self, zf):
        rows = self._get_file(zf, "residential", "res_detail")
        if not rows:
            return
        for row in rows:
            acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT", "prop_id")
            if acct:
                self._res_details[acct] = {
                    "year_built": g(row, "YEAR_BUILT", "YR_BUILT", "BUILT",
                                    "ACT_YR_BLT"),
                    "sqft":       g(row, "BLDG_SQFT", "SQFT", "LIVING_AREA",
                                    "TOTAL_SQFT", "LVG_AREA"),
                    "bedrooms":   g(row, "BEDROOMS", "BED", "BED_RM", "NO_BED"),
                    "bathrooms":  g(row, "BATHROOMS", "BATH", "FULL_BATH", "NO_BATH"),
                    "stories":    g(row, "STORIES", "STORY", "NUM_STORIES", "NO_STORY"),
                    "garage":     g(row, "GARAGE", "GAR_SPACES", "GARAGE_CAP",
                                    "GAR_CD"),
                    "pool":       g(row, "POOL", "HAS_POOL", "POOL_CD"),
                }
        log.info(f"Residential detail records: {len(self._res_details):,}")

    def _parse_accounts(self, zf):
        rows = self._get_file(zf, "account_info", "account", "acct", "owner")
        if not rows:
            log.error("Could not find main account file!")
            return

        log.info(f"Processing {len(rows):,} account rows …")
        processed = 0

        for row in rows:
            try:
                acct = g(row, "ACCOUNT_NUM", "ACCT_NUM", "ACCOUNT", "ACCT",
                         "prop_id", "PROP_ID")
                if not acct:
                    continue

                state_cd = g(row, "STATE_CD", "STATECODE", "CAT_CD",
                             "CATEGORY", "PROP_TYPE", "SPTD_CODE").upper()

                is_residential = (
                    acct in self._res_accounts or
                    (state_cd and state_cd[:1] in self.RESIDENTIAL_CODES)
                )
                if not is_residential:
                    continue

                # Owner name
                owner = g(row, "OWNER_NAME", "OWNER1", "OWN_NAME", "NAME",
                          "OWNR_NAME", "OWNER", "GEO_ID").title()

                # Property address — try combined field first, then parts
                prop_address = g(row, "SITUS_ADDRESS", "SITE_ADDRESS",
                                 "PROP_ADDRESS", "SITUS_ADDR")
                if not prop_address:
                    parts = [
                        g(row, "SITUS_NUM", "PROP_NUM", "STREET_NUM", "STR_NUM"),
                        g(row, "SITUS_DIR", "PROP_DIR", "STREET_DIR"),
                        g(row, "SITUS_STREET", "PROP_STREET", "STREET_NAME",
                          "STR_NAME", "SITUS_STR"),
                        g(row, "SITUS_SUFFIX", "STREET_SUFFIX", "STR_SFX"),
                        g(row, "SITUS_UNIT", "UNIT_NUM", "APT"),
                    ]
                    prop_address = " ".join(p for p in parts if p).title()

                prop_city = g(row, "SITUS_CITY", "PROP_CITY", "CITY",
                              "SITE_CITY", "GEO_CITY").title()
                prop_zip  = g(row, "SITUS_ZIP", "PROP_ZIP", "ZIP",
                              "SITE_ZIP", "GEO_ZIP")

                # Mailing address
                mail_addr  = g(row, "MAIL_ADDR1", "MAIL_ADD1", "MAIL_LINE1",
                               "MAILING_ADDR", "MAIL_ADDRESS",
                               "ADDR1", "ADDRESS1").title()
                mail_addr2 = g(row, "MAIL_ADDR2", "MAIL_ADD2", "MAIL_LINE2",
                               "ADDR2", "ADDRESS2").title()
                if mail_addr2:
                    mail_addr = f"{mail_addr} {mail_addr2}".strip()
                mail_city  = g(row, "MAIL_CITY", "MAILCITY", "MAIL_CTY",
                               "CITY2").title()
                mail_state = g(row, "MAIL_STATE", "MAILSTATE", "MAIL_ST",
                               "STATE2").upper()
                mail_zip   = g(row, "MAIL_ZIP", "MAILZIP", "MAIL_ZIPCODE",
                               "ZIP2")

                # Values
                val_data  = self._values.get(acct, {})
                appraised = val_data.get("appraised") or safe_float(
                    g(row, "APPRAISED_VAL", "APPR_VAL", "TOTAL_VAL",
                      "MARKET_VAL", "MKT_VAL", "TOT_VAL"))
                prior_val = val_data.get("prior_val")

                # Exemptions
                ex_data       = self._exemptions.get(acct, {})
                has_homestead = ex_data.get("has_homestead", False)
                ex_code       = g(row, "HOMESTEAD", "HS_FLAG", "HS_CAP",
                                  "EXEMPTIONS").upper()
                if ex_code in ("Y", "YES", "1", "HS"):
                    has_homestead = True

                # Residential details
                res = self._res_details.get(acct, {})

                # Internal scoring flags
                tax_delinquent    = acct in self._tax_delinquent
                value_dropped     = (
                    appraised is not None and prior_val is not None and
                    prior_val > 0 and appraised < prior_val * 0.90
                )
                homestead_removed = (
                    not has_homestead and
                    appraised is not None and appraised > 50_000
                )

                record = {
                    "doc_num":         acct,
                    "doc_type":        "CAD",
                    "filed":           datetime.now().strftime("%Y-%m-%d"),
                    "cat":             "CAD",
                    "cat_label":       "Property Record",
                    "owner":           owner,
                    "grantee":         "",
                    "amount":          appraised,
                    "legal":           g(row, "LEGAL_DESC", "LEGAL",
                                         "LEGAL_DESCRIPTION", "GEO_DESCRIPTION"),
                    "clerk_url":       f"https://www.dallascad.org/AcctDetailRes.aspx?crypt={acct}",
                    "prop_address":    prop_address,
                    "prop_city":       prop_city or "Dallas",
                    "prop_state":      "TX",
                    "prop_zip":        prop_zip,
                    "mail_address":    mail_addr,
                    "mail_city":       mail_city,
                    "mail_state":      mail_state or "TX",
                    "mail_zip":        mail_zip,
                    "year_built":      res.get("year_built", ""),
                    "sqft":            res.get("sqft", ""),
                    "bedrooms":        res.get("bedrooms", ""),
                    "bathrooms":       res.get("bathrooms", ""),
                    "stories":         res.get("stories", ""),
                    "garage":          res.get("garage", ""),
                    "pool":            res.get("pool", ""),
                    "appraised_value": appraised,
                    "land_value":      val_data.get("land"),
                    "impr_value":      val_data.get("impr"),
                    "state_cd":        state_cd,
                    "_tax_delinquent":    tax_delinquent,
                    "_has_homestead":     has_homestead,
                    "_homestead_removed": homestead_removed,
                    "_appraised_value":   appraised,
                    "_value_dropped":     value_dropped,
                    "flags": [],
                    "score": 0,
                }

                self.records.append(record)
                processed += 1
                if processed % 50_000 == 0:
                    log.info(f"  Processed {processed:,} residential records …")

            except Exception as exc:
                log.debug(f"Row error: {exc}")

        log.info(f"Total residential records: {processed:,}")


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────

def score_record(record: dict) -> dict:
    flags = []
    score = 20

    for label, bonus, pred in FLAG_DEFS:
        try:
            if pred(record):
                flags.append(label)
                score += bonus
        except Exception:
            pass

    if len(flags) >= 3:
        score += 10
    if len(flags) >= 5:
        score += 10

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
    "Appraised Value", "Seller Score", "Motivated Seller Flags",
    "Year Built", "Sq Ft", "Bedrooms", "Bathrooms",
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
                "Appraised Value":        r.get("appraised_value") or "",
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Year Built":             r.get("year_built", ""),
                "Sq Ft":                  r.get("sqft", ""),
                "Bedrooms":               r.get("bedrooms", ""),
                "Bathrooms":              r.get("bathrooms", ""),
                "Source":                 "Dallas CAD (dallascad.org)",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info(f"GHL CSV → {path}  ({len(records)} rows)")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now(timezone.utc)

    log.info("=" * 65)
    log.info("  Dallas County CAD — Motivated Seller Lead Scraper")
    log.info(f"  Source  : {CAD_PAGE}")
    log.info(f"  Run at  : {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 65)

    # 1. Download
    downloader = DCADDownloader()
    zip_bytes  = downloader.get_zip()
    if not zip_bytes:
        log.error("Failed to download DCAD data. Exiting.")
        sys.exit(1)

    # 2. Parse
    parser  = DCADParser(zip_bytes)
    records = parser.parse()
    if not records:
        log.error("No records parsed. Exiting.")
        sys.exit(1)

    # 3. Score
    log.info("Scoring records …")
    for r in records:
        score_record(r)

    # 4. Filter to motivated sellers (score > 30 = at least one real flag)
    motivated = [r for r in records if r["score"] > 30]
    log.info(f"Motivated seller leads: {len(motivated):,} of {len(records):,}")

    # Sort by score
    motivated.sort(key=lambda x: x["score"], reverse=True)

    # 5. Clean internal keys
    clean = []
    for r in motivated:
        clean.append({k: v for k, v in r.items() if not k.startswith("_")})

    # 6. Write JSON (top 5000)
    with_addr = sum(1 for r in clean if r.get("prop_address") or r.get("mail_address"))
    payload = {
        "fetched_at": now.isoformat(),
        "source":     "Dallas Central Appraisal District (dallascad.org)",
        "date_range": {
            "start": now.strftime("%Y-%m-%d"),
            "end":   now.strftime("%Y-%m-%d"),
        },
        "total":        len(clean),
        "showing":      min(len(clean), 5000),
        "with_address": with_addr,
        "records":      clean[:5000],
    }
    for out in (DASHBOARD_JSON, DATA_JSON):
        out.write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info(f"JSON → {out}")

    # 7. GHL CSV (top 10,000)
    write_ghl_csv(clean[:10_000], GHL_CSV)

    # 8. Summary
    log.info("=" * 65)
    log.info(f"  Done. {len(clean):,} leads | {with_addr:,} with address")
    log.info(f"  Top score: {clean[0]['score'] if clean else 0}")
    log.info("  Flag breakdown:")
    flag_counts: dict[str, int] = {}
    for r in clean:
        for f in r.get("flags", []):
            flag_counts[f] = flag_counts.get(f, 0) + 1
    for flag, count in sorted(flag_counts.items(), key=lambda x: -x[1]):
        log.info(f"    {flag}: {count:,}")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
