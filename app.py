"""
================================================================================
 NANI ARMENIAN NATIONAL FOOD — EXECUTIVE SALES DASHBOARD
================================================================================
 A production-grade Streamlit dashboard for restaurant executive sales
 reporting, sourced directly from the iiko OLAP Sales API.

 Author : Executive Python Developer / Hospitality CFO Systems Architect
 Target : Streamlit >= 1.32, Python >= 3.9

 ------------------------------------------------------------------------------
 QUICK START
 ------------------------------------------------------------------------------
   pip install streamlit pandas requests plotly xlsxwriter
   cp .env.example .env          # then fill in your credentials
   streamlit run app.py

 Headless nightly sync (for cron / Task Scheduler):
   python app.py sync            # incremental (last N days)
   python app.py sync --full     # full year-to-date rebuild
   python app.py doctor          # connectivity + endpoint diagnostics
   python app.py reset-endpoints # forget discovered paths and probe again

 ------------------------------------------------------------------------------
 THE OLAP ENDPOINT
 ------------------------------------------------------------------------------
 Captured from the iikoWeb portal's own network traffic:

     POST /api/olap/fetch/{preset_id}/grouped-table

 The preset id lives in the PATH, so the route cannot be called without one.
 DEFAULT_PRESET_ID holds the "Sales intr" id; IIKO_PRESET_ID overrides it.
 Every report request goes to this route first, always — the login dialect does
 not get a say, and neither does a cached value.

 /api/reports/olap is in BLOCKED_ENDPOINT_PREFIXES and is never contacted,
 along with every path beneath it: this host answers 404 "No route found" to
 that whole route. Empty that list to re-enable it.

 The remaining entries in OLAP_ENDPOINTS, PRESET_LIST_ENDPOINTS and
 PRESET_RUN_ENDPOINTS are fallbacks. Whatever works is remembered in `meta`.

 The request BODY was not captured, so _preset_bodies() tries the common date
 shapes in turn ({"dateFrom","dateTo"} first) and keeps whichever the server
 accepts. Once you know the real payload, delete the others.

 The RESPONSE is a grouped table rather than a flat list, so _extract_rows
 handles both a columns + array-of-arrays table and a nested group tree, pushing
 each group's dimension values down onto its leaf rows.

 To force one path and skip discovery, set IIKO_OLAP_ENDPOINT in .env.

 Example crontab entry (every morning at 06:15 local time):
   15 6 * * *  cd /path/to/dashboard && /usr/bin/python3 app.py sync

 ------------------------------------------------------------------------------
 CREDENTIALS
 ------------------------------------------------------------------------------
 Credentials are read, in order of precedence, from:
   1. Environment variables
   2. A local .env file sitting next to this script
   3. Streamlit secrets (.streamlit/secrets.toml)
   4. The built-in defaults at the bottom of the CONFIG block

 Storing secrets in source control is strongly discouraged. Prefer .env.
================================================================================
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import io
import logging
import os
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

# Plotly is only needed for the UI; the headless `sync` CLI must not require it.
try:
    import plotly.graph_objects as go
    import plotly.express as px

    _HAS_PLOTLY = True
except Exception:  # pragma: no cover
    go = None  # type: ignore
    px = None  # type: ignore
    _HAS_PLOTLY = False

# Streamlit is likewise optional so the CLI can run on a bare server.
try:
    import streamlit as st

    _HAS_STREAMLIT = True
except Exception:  # pragma: no cover
    st = None  # type: ignore
    _HAS_STREAMLIT = False


APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "nani_sales.db"
LOG_PATH = DATA_DIR / "sync.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_PATH, encoding="utf-8")],
)
log = logging.getLogger("nani")


# ==============================================================================
#  SECTION 1 — CONFIGURATION
# ==============================================================================

def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (no external dependency)."""
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)
    except Exception as exc:  # pragma: no cover
        log.warning("Could not parse .env: %s", exc)


_load_dotenv(APP_DIR / ".env")


def _cfg(key: str, default: str = "") -> str:
    """Resolve a setting from env -> streamlit secrets -> default."""
    val = os.environ.get(key)
    if val:
        return val
    if _HAS_STREAMLIT:
        try:
            secret = st.secrets.get(key)  # type: ignore[union-attr]
            if secret:
                return str(secret)
        except Exception:
            pass
    return default


# The "Sales intr" preset on chinatown.iikoweb.ru, captured from the portal's
# own network traffic. IIKO_PRESET_ID in the environment or .env overrides it.
# The OLAP route carries this id in its PATH, so it is required, not optional.
DEFAULT_PRESET_ID = "7ac4dfd850327a8d89ab49c7000ba4d93a5263ce"


@dataclass
class Config:
    base_url: str = field(default_factory=lambda: _cfg("IIKO_BASE_URL", "https://chinatown.iikoweb.ru").rstrip("/"))
    login: str = field(default_factory=lambda: _cfg("IIKO_LOGIN", "Edgar"))
    password: str = field(default_factory=lambda: _cfg("IIKO_PASSWORD", ""))
    api_key: str = field(default_factory=lambda: _cfg("IIKO_API_KEY", ""))
    restaurant: str = field(default_factory=lambda: _cfg("IIKO_RESTAURANT", "Nani"))
    preset_name: str = field(default_factory=lambda: _cfg("IIKO_PRESET_NAME", "Sales intr"))
    # Read straight from IIKO_PRESET_ID (environment first, then .env, then
    # Streamlit secrets), falling back to the captured id so a clean checkout
    # works with no configuration at all.
    preset_id: str = field(default_factory=lambda: _cfg("IIKO_PRESET_ID", DEFAULT_PRESET_ID))
    # How many trailing days to re-pull on every incremental sync. iiko lets
    # operators amend closed checks for a few days, so we always re-read a
    # short tail rather than trusting yesterday's numbers as final.
    incremental_tail_days: int = field(default_factory=lambda: int(_cfg("IIKO_TAIL_DAYS", "7")))
    request_timeout: int = field(default_factory=lambda: int(_cfg("IIKO_TIMEOUT", "120")))
    # Split very long date ranges into chunks so the OLAP server is not asked
    # for a full year in a single request.
    chunk_days: int = field(default_factory=lambda: int(_cfg("IIKO_CHUNK_DAYS", "62")))
    verify_ssl: bool = field(default_factory=lambda: _cfg("IIKO_VERIFY_SSL", "1") not in ("0", "false", "False"))

    # --- endpoint pinning ---------------------------------------------------
    # Set any of these to force a path and skip discovery for it entirely.
    # Leave blank to let the client probe (it leads with the resto path).
    olap_endpoint: str = field(default_factory=lambda: _cfg("IIKO_OLAP_ENDPOINT", ""))
    presets_endpoint: str = field(default_factory=lambda: _cfg("IIKO_PRESETS_ENDPOINT", ""))
    preset_run_endpoint: str = field(default_factory=lambda: _cfg("IIKO_PRESET_RUN_ENDPOINT", ""))

    def masked(self) -> Dict[str, str]:
        return {
            "base_url": self.base_url,
            "login": self.login,
            "password": "•" * 8 if self.password else "(not set)",
            "api_key": (self.api_key[:6] + "…" + self.api_key[-4:]) if len(self.api_key) > 12 else ("(set)" if self.api_key else "(not set)"),
            "restaurant": self.restaurant,
            "preset": self.preset_name or self.preset_id or "(none)",
            "olap_pin": self.olap_endpoint or "(auto-discover)",
        }


CFG = Config()

APP_VERSION = "1.4.0"

CURRENCY = "AMD"
CURRENCY_SYMBOL = "֏"


# ==============================================================================
#  SECTION 2 — DESIGN SYSTEM
# ==============================================================================

class Palette:
    """Executive CFO luxury-minimalist palette."""

    OBSIDIAN = "#0F172A"      # page background
    SLATE = "#1E293B"         # card surface
    SLATE_2 = "#273449"       # elevated surface / borders
    SLATE_3 = "#334155"       # hairlines
    GOLD = "#D4AF37"          # primary accent
    GOLD_SOFT = "#E8CE73"
    EMERALD = "#10B981"       # gains
    CRIMSON = "#EF4444"       # losses
    TEXT = "#F1F5F9"
    TEXT_MUTED = "#94A3B8"
    TEXT_FAINT = "#64748B"

    # Channel identity colours (kept distinct and colour-blind separable).
    CH_HALL = "#D4AF37"       # gold — the house
    CH_YANDEX = "#FCE000"     # Yandex Eats yellow
    CH_GLOVO = "#00A082"      # Glovo teal
    CH_BUYAM = "#E5136B"      # buy.am magenta
    CH_COMPLIMENTS = "#64748B"


CHANNEL_COLORS = {
    "Hall": Palette.CH_HALL,
    "Yandex": Palette.CH_YANDEX,
    "Glovo": Palette.CH_GLOVO,
    "Buy.am": Palette.CH_BUYAM,
    "Compliments": Palette.CH_COMPLIMENTS,
}

# Brand marks, embedded so the dashboard stays a single portable file.
LOGO_B64: Dict[str, str] = {
    "Nani": "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",
    "Buy.am": "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",
    "Glovo": "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",
    "Yandex": "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",
}


def logo_uri(name: str) -> str:
    b64 = LOGO_B64.get(name, "")
    return f"data:image/png;base64,{b64}" if b64 else ""


# ==============================================================================
#  SECTION 3 — FIELD MAPPING & CHANNEL RULE ENGINE
# ==============================================================================

# Canonical internal schema used everywhere downstream:
#   business_date : datetime.date
#   payment_type  : str   (raw ТипОплатКомб value)
#   channel       : str   (Hall | Yandex | Glovo | Buy.am | Compliments)
#   order_count   : int
#   gross_sales   : float (Sales before discount)
#   net_sales     : float (Sales after discount)
#   cogs          : float (Cost of goods sold, AMD)

# Russian OLAP headers -> canonical names. Keys are normalised (lowercase,
# punctuation and whitespace stripped) so "Себест (%)" and "Себест(%)" both hit.
FIELD_ALIASES: Dict[str, str] = {
    # --- order count -------------------------------------------------------
    "заказы": "order_count",
    "количествозаказов": "order_count",
    "колвозаказов": "order_count",
    "uniqorderidorderscount": "order_count",
    "ordercount": "order_count",
    "checks": "order_count",
    # --- gross sales (before discount) -------------------------------------
    "сумбезскид": "gross_sales",
    "суммабезскидки": "gross_sales",
    "суммбезскидки": "gross_sales",
    "dishsumint": "gross_sales",
    "grosssales": "gross_sales",
    # --- net sales (after discount) ----------------------------------------
    "суммаопл": "net_sales",
    "суммаоплаты": "net_sales",
    "суммасоскидкой": "net_sales",
    "dishdiscountsumint": "net_sales",
    "netsales": "net_sales",
    # --- cost of goods -----------------------------------------------------
    "себест": "cogs",
    "себестоимость": "cogs",
    "productcostbaseproductcost": "cogs",
    "cogs": "cogs",
    # --- derived, recomputed but accepted if supplied ----------------------
    "себест%": "cogs_pct_src",
    "себестоимость%": "cogs_pct_src",
    "срчек": "avg_check_src",
    "среднийчек": "avg_check_src",
    # --- dimensions --------------------------------------------------------
    "типоплаткомб": "payment_type",
    "типоплаты": "payment_type",
    "paytypescombo": "payment_type",
    "paytype": "payment_type",
    "учетндень": "business_date",
    "учетнден": "business_date",
    "учетныйдень": "business_date",
    "деньучета": "business_date",
    "датаучета": "business_date",
    "дата": "business_date",
    "датазакрытия": "business_date",
    "датаоткрытия": "business_date",
    "opendatetyped": "business_date",
    "opendate": "business_date",
    "closetimetyped": "business_date",
    "closedate": "business_date",
    "businessday": "business_date",
    "date": "business_date",
    # --- venue -------------------------------------------------------------
    "ресторан": "venue",
    "подразделение": "venue",
    "department": "venue",
    "restorauntgroup": "venue",
}


# Canonical names map to themselves, so re-normalising an already-clean frame
# (a cache reload, a re-import) is an identity operation rather than a guess.
FIELD_ALIASES.update({
    "businessdate": "business_date",
    "paymenttype": "payment_type",
    "ordercount": "order_count",
    "grosssales": "gross_sales",
    "netsales": "net_sales",
    "channel": "channel",
    "venue": "venue",
})


def _norm_key(s: str) -> str:
    """Normalise a column header for alias lookup."""
    s = str(s).strip().lower()
    s = s.replace("ё", "е")
    s = re.sub(r"[\s\.\-_ ]+", "", s)
    s = s.replace("(", "").replace(")", "")
    return s


# Channel rules are evaluated top-down; first match wins. Compliments must be
# tested before everything else so a free-of-charge Yandex order is booked as a
# compliment rather than as delivery revenue.
CHANNEL_RULES: List[Tuple[str, List[str]]] = [
    ("Compliments", [r"без\s*оплат", r"безоплат", r"complim", r"компл", r"бесплат", r"\bfree\b"]),
    ("Yandex", [r"yandex", r"яндекс", r"я\.?еда", r"yango"]),
    ("Glovo", [r"glovo", r"глово"]),
    ("Buy.am", [r"buy\s*\.?\s*am", r"бай\s*\.?\s*ам", r"\bbuy\b"]),
]

DELIVERY_CHANNELS = ["Yandex", "Glovo", "Buy.am"]
REVENUE_CHANNELS = ["Hall"] + DELIVERY_CHANNELS
ALL_CHANNELS = REVENUE_CHANNELS + ["Compliments"]


def classify_channel(payment_type: Any) -> str:
    """Map a raw ТипОплатКомб string onto one of the five business channels."""
    raw = "" if payment_type is None else str(payment_type)
    probe = raw.strip().lower().replace("ё", "е")
    if not probe:
        return "Hall"
    for channel, patterns in CHANNEL_RULES:
        for pat in patterns:
            if re.search(pat, probe, flags=re.IGNORECASE):
                return channel
    # Catch-all: cards, cash, Idram, Փոխանցումով (bank transfer), vouchers,
    # mixed tenders and anything else the venue starts accepting tomorrow.
    return "Hall"


# ==============================================================================
#  SECTION 4 — iiko API CLIENT
# ==============================================================================

class IikoError(RuntimeError):
    """Any failure while talking to the iiko backend."""


class IikoAuthError(IikoError):
    """Credentials rejected, or the token could not be obtained."""


# ------------------------------------------------------------------------------
#  THE OLAP REPORT ADDRESS
# ------------------------------------------------------------------------------
#  Every OLAP query goes here first — always, regardless of which dialect
#  authenticated or what a previous run cached. Change this one line to move the
#  report address; everything else follows it.
#
#  Captured from the iikoWeb portal's own network traffic while opening the
#  "Sales intr" report:
#      POST /api/olap/fetch/{preset_id}/grouped-table
#  The preset id is part of the PATH, not the body, so this route cannot be
#  called without one — see DEFAULT_PRESET_ID and IIKO_PRESET_ID above.
PRIMARY_OLAP_ENDPOINT = "/api/olap/fetch/{preset_id}/grouped-table"
PRIMARY_PRESET_RUN_ENDPOINT = "/api/olap/fetch/{preset_id}/grouped-table"
PRIMARY_PRESET_LIST_ENDPOINT = "/api/olap/presets"

#  Routes that are never contacted, whatever nominates them — a stale value
#  remembered by an older build, a dialect default, or the catalogue.
#  chinatown.iikoweb.ru answers 404 "No route found" to this prefix and to every
#  path beneath it, so probing it only writes a misleading error to the log.
#  Empty this list to re-enable them. An explicit IIKO_*_ENDPOINT pin still wins.
BLOCKED_ENDPOINT_PREFIXES: List[str] = [
    "/api/reports/olap",
]


def _is_blocked(path: str) -> bool:
    return any(path.startswith(prefix) for prefix in BLOCKED_ENDPOINT_PREFIXES)


# The iiko estate ships several incompatible HTTP surfaces depending on whether
# the host is an iikoRMS "resto" server, an iikoWeb portal, or iikoCloud. Rather
# than hard-coding one, the client probes each dialect in turn and remembers
# whichever answered. Note that a dialect decides only how to LOG IN; report
# paths come from the catalogues below.
DIALECTS: List[Dict[str, Any]] = [
    {
        "name": "resto",
        "auth": "/resto/api/auth",
        "logout": "/resto/api/logout",
        "olap": "/resto/api/v2/reports/olap",
        "presets": "/resto/api/v2/reports/olap/presets",
        "preset_run": "/resto/api/v2/reports/olap/byPresetId/{preset_id}",
        "token_param": "key",
        "password_style": "sha1",
    },
    {
        "name": "resto-plain",
        "auth": "/resto/api/auth",
        "logout": "/resto/api/logout",
        "olap": "/resto/api/v2/reports/olap",
        "presets": "/resto/api/v2/reports/olap/presets",
        "preset_run": "/resto/api/v2/reports/olap/byPresetId/{preset_id}",
        "token_param": "key",
        "password_style": "plain",
    },
    {
        "name": "iikoweb",
        "auth": "/api/auth/login",
        "logout": "/api/auth/logout",
        # This dialect's login works on chinatown.iikoweb.ru; its report paths
        # do not. Report paths are chosen by the catalogue, never from here.
        "olap": PRIMARY_OLAP_ENDPOINT,
        "presets": PRIMARY_PRESET_LIST_ENDPOINT,
        "preset_run": PRIMARY_PRESET_RUN_ENDPOINT,
        "token_param": "bearer",
        "password_style": "plain",
    },
    {
        "name": "iikoweb-v2",
        "auth": "/api/v2/auth/login",
        "logout": "/api/v2/auth/logout",
        "olap": "/api/v2/reports/olap",
        "presets": "/api/v2/reports/olap/presets",
        "preset_run": "/api/v2/reports/olap/preset/{preset_id}",
        "token_param": "bearer",
        "password_style": "plain",
    },
]

# --- Endpoint discovery -------------------------------------------------------
# Authenticating against one dialect does NOT guarantee its report paths exist:
# chinatown.iikoweb.ru accepts POST /api/auth/login but answers 404 to
# POST /api/reports/olap. So report paths are discovered independently of the
# login path, and the winner is remembered in the meta table.

# A path containing {preset_id} is preset-scoped: it is only usable once a
# preset id is known, and it is skipped when there is none.
OLAP_ENDPOINTS: List[str] = [
    PRIMARY_OLAP_ENDPOINT,                    # ← primary (verified in DevTools)
    f"/api/olap/fetch/{DEFAULT_PRESET_ID}/grouped-table",   # literal, no templating
    "/api/olap/fetch/{preset_id}/table",
    "/resto/api/v2/reports/olap",
    "/api/v2/reports/olap",
    "/api/v1/reports/olap",
    "/resto/api/reports/olap",
    "/api/olap/report",
    "/api/reports/sales/olap",
    "/api/olap",
    "/reports/olap",
]

PRESET_LIST_ENDPOINTS: List[str] = [
    PRIMARY_PRESET_LIST_ENDPOINT,
    "/api/olap/presets/list",
    "/resto/api/v2/reports/olap/presets",
    "/api/v2/reports/olap/presets",
    "/api/v1/reports/olap/presets",
    "/api/reports/presets",
    "/reports/olap/presets",
]

PRESET_RUN_ENDPOINTS: List[str] = [
    PRIMARY_PRESET_RUN_ENDPOINT,              # ← primary (verified in DevTools)
    "/api/olap/fetch/{preset_id}/table",
    "/api/olap/fetch/{preset_id}",
    "/resto/api/v2/reports/olap/byPresetId/{preset_id}",
    "/api/v2/reports/olap/preset/{preset_id}",
    "/api/olap/presets/{preset_id}",
]

# Fail loudly at import if an edit ever demotes the primary path or lets a
# blocked route back into a catalogue. Cheaper than rediscovering it in prod.
assert OLAP_ENDPOINTS[0] == PRIMARY_OLAP_ENDPOINT, "primary OLAP endpoint must lead"
assert PRESET_LIST_ENDPOINTS[0] == PRIMARY_PRESET_LIST_ENDPOINT
assert PRESET_RUN_ENDPOINTS[0] == PRIMARY_PRESET_RUN_ENDPOINT
assert not any(
    _is_blocked(p)
    for p in OLAP_ENDPOINTS + PRESET_LIST_ENDPOINTS + PRESET_RUN_ENDPOINTS
), "a blocked route is present in an endpoint catalogue"

# HTTP statuses that mean "wrong address, try the next candidate" rather than
# "this endpoint is right but the request was bad".
_ROUTE_MISS = {404, 405, 501}


def _join_url(base: str, path: str) -> str:
    """Join a base URL and a path without doubling or dropping the slash."""
    return f"{base.rstrip('/')}/{str(path).lstrip('/')}"


def _ordered_candidates(pinned: Optional[str], remembered: Optional[str],
                        catalogue: Sequence[str]) -> List[str]:
    """Candidate paths, most-likely first, with no duplicates.

    Order: a path pinned in configuration, then one proven to work on a previous
    run, then the catalogue in its declared order.

    Note what is deliberately *absent*: the report path belonging to whichever
    dialect authenticated. chinatown.iikoweb.ru accepts POST /api/auth/login yet
    answers 404 to POST /api/reports/olap, so letting the login dialect nominate
    the report path just puts a known-dead address at the front of the queue.
    The catalogue order is authoritative instead, and it leads with
    /resto/api/v2/reports/olap.

    Anything matching BLOCKED_ENDPOINT_PREFIXES is dropped here, including a
    stale remembered value — so a path that has been retired cannot come back
    from a cache written by an older build. An explicit configuration pin is
    the one exception: that is a deliberate operator decision.
    """
    out: List[str] = []
    if pinned:
        out.append(pinned)                       # pins bypass the block list
    for item in (remembered,):
        if item and item not in out and not _is_blocked(item):
            out.append(item)
    for item in catalogue:
        if item not in out and not _is_blocked(item):
            out.append(item)
    return out


# Standard iiko OLAP SALES field identifiers, used when we have to build the
# report ourselves instead of running a saved preset.
OLAP_ROW_FIELDS = ["OpenDate.Typed", "PayTypes.Combo"]
OLAP_AGG_FIELDS = [
    "DishSumInt",                     # Сумма без скидки  -> gross
    "DishDiscountSumInt",             # Сумма со скидкой  -> net
    "ProductCostBase.ProductCost",    # Себестоимость     -> COGS
    "UniqOrderId.OrdersCount",        # Заказы            -> checks
]


def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


class IikoClient:
    """Resilient client for the iiko OLAP Sales report."""

    def __init__(self, cfg: Config = CFG):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "Nani-Executive-Dashboard/1.0",
        })
        self.token: Optional[str] = None
        self.dialect: Optional[Dict[str, Any]] = None
        # Report paths are discovered at run time and may belong to a different
        # dialect than the one that authenticated.
        self.olap_endpoint: Optional[str] = None
        self.presets_endpoint: Optional[str] = None
        self.preset_run_endpoint: Optional[str] = None
        self._lock = threading.Lock()
        self._diagnostics: List[str] = []

    # -- plumbing ----------------------------------------------------------
    def _url(self, path: str) -> str:
        return _join_url(self.cfg.base_url, path)

    def _note(self, msg: str) -> None:
        self._diagnostics.append(msg)
        log.debug(msg)

    @property
    def diagnostics(self) -> List[str]:
        return list(self._diagnostics)

    def _auth_params(self) -> Dict[str, str]:
        """Query params carrying the token.

        The token is presented both as ``?key=`` and as a bearer header on every
        authenticated call. An endpoint from one dialect may well be served by a
        host whose login belongs to another — which is exactly the situation on
        chinatown.iikoweb.ru — and a server simply ignores the credential form
        it does not use.
        """
        return {"key": self.token} if self.token else {}

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        url = self._url(path)
        p = dict(params or {})
        p.update(self._auth_params())
        headers = self._auth_headers()
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        return self.session.request(
            method,
            url,
            params=p or None,
            json=json_body,
            headers=headers or None,
            timeout=timeout or self.cfg.request_timeout,
            verify=self.cfg.verify_ssl,
        )

    # -- authentication ----------------------------------------------------
    def authenticate(self) -> None:
        """Try each dialect until one hands back a usable token."""
        with self._lock:
            if self.token and self.dialect:
                return
            if not self.cfg.password and not self.cfg.api_key:
                raise IikoAuthError(
                    "No password or API key configured. Set IIKO_PASSWORD (and/or "
                    "IIKO_API_KEY) in your environment or .env file."
                )

            remembered = _meta_get("api_dialect")
            ordered = sorted(DIALECTS, key=lambda d: 0 if d["name"] == remembered else 1)

            errors: List[str] = []
            for dialect in ordered:
                try:
                    token = self._try_auth(dialect)
                except Exception as exc:
                    errors.append(f"{dialect['name']}: {exc}")
                    self._note(f"auth dialect '{dialect['name']}' failed: {exc}")
                    continue
                if token:
                    self.token = token
                    self.dialect = dialect
                    _meta_set("api_dialect", dialect["name"])
                    self._note(f"authenticated via dialect '{dialect['name']}'")
                    log.info("iiko: authenticated using the '%s' API dialect", dialect["name"])
                    return

            raise IikoAuthError(
                "Could not authenticate against "
                f"{self.cfg.base_url} with any known iiko API dialect.\n  "
                + "\n  ".join(errors)
                + "\n\nIf this host is an iikoWeb portal rather than an iikoRMS server, "
                "the OLAP endpoints differ — set IIKO_BASE_URL to the RMS server, or "
                "run `python app.py doctor` for a full probe."
            )

    def _try_auth(self, dialect: Dict[str, Any]) -> Optional[str]:
        path = dialect["auth"]
        style = dialect["password_style"]
        pwd = self.cfg.password
        secret = _sha1(pwd) if style == "sha1" else pwd

        if dialect["token_param"] == "key":
            # iikoRMS: GET /resto/api/auth?login=..&pass=..  -> bare token string
            resp = self._request("GET", path, params={"login": self.cfg.login, "pass": secret}, timeout=45)
            if resp.status_code != 200:
                raise IikoError(f"HTTP {resp.status_code} {resp.text[:160]}")
            token = resp.text.strip().strip('"')
            if not token or "<" in token or len(token) > 200:
                raise IikoError(f"unexpected auth payload: {token[:120]!r}")
            return token

        # iikoWeb style: POST JSON credentials, token somewhere in the response.
        payloads = [
            {"login": self.cfg.login, "password": pwd},
            {"userName": self.cfg.login, "password": pwd},
            {"apiLogin": self.cfg.api_key} if self.cfg.api_key else None,
        ]
        last = ""
        for body in [b for b in payloads if b]:
            resp = self._request("POST", path, json_body=body, timeout=45)
            last = f"HTTP {resp.status_code} {resp.text[:160]}"
            if resp.status_code not in (200, 201):
                continue
            try:
                data = resp.json()
            except ValueError:
                continue
            token = _dig(data, ["token", "accessToken", "access_token", "key", "sessionKey", "jwt"])
            if token:
                return str(token)
        raise IikoError(last or "no token in response")

    def logout(self) -> None:
        if not (self.token and self.dialect):
            return
        with contextlib.suppress(Exception):
            self._request("GET", self.dialect["logout"], timeout=20)
        self.token = None

    # -- endpoint discovery ------------------------------------------------
    def _discover(
        self,
        kind: str,
        catalogue: Sequence[str],
        probe: Any,
        *,
        format_args: Optional[Dict[str, str]] = None,
    ) -> Tuple[Optional[str], Any, List[str]]:
        """Walk candidate paths until one is actually routed.

        `probe(path) -> (ok, payload, note)`. A 404/405/501 means the path does
        not exist on this host, so we move on; any other outcome is reported.
        Returns (winning_path_template, payload, attempt_log).
        """
        assert self.dialect
        pinned = {
            "olap": self.cfg.olap_endpoint,
            "presets": self.cfg.presets_endpoint,
            "preset_run": self.cfg.preset_run_endpoint,
        }.get(kind, "")
        remembered = _meta_get(f"endpoint_{kind}")
        attempts: List[str] = []

        for template in _ordered_candidates(pinned, remembered, catalogue):
            if "{preset_id}" in template:
                # Preset-scoped route: unusable until an id is known.
                if not (format_args or {}).get("preset_id"):
                    attempts.append(f"{template}: skipped (no preset id available)")
                    continue
                path = template.format(**format_args)
            else:
                path = template
            try:
                ok, payload, note = probe(path)
            except requests.RequestException as exc:
                attempts.append(f"{path}: {exc}")
                log.info("iiko: %s candidate %s -> %s", kind, path, exc)
                continue
            if ok:
                if template != remembered:
                    _meta_set(f"endpoint_{kind}", template)
                    self._note(f"discovered {kind} endpoint: {template}")
                    log.info("iiko: %s endpoint resolved to %s", kind, template)
                return template, payload, attempts
            attempts.append(f"{path}: {note}")
            # Logged at INFO, not DEBUG: when a host answers 404 the operator
            # needs to see the fall-through happening, not just the final error.
            log.info("iiko: %s candidate %s -> %s (trying next)", kind, path, note)
        return None, None, attempts

    # -- presets -----------------------------------------------------------
    def list_presets(self) -> List[Dict[str, Any]]:
        self.authenticate()

        def probe(path: str) -> Tuple[bool, Any, str]:
            resp = self._request("GET", path, timeout=60)
            if resp.status_code in _ROUTE_MISS:
                return False, None, f"HTTP {resp.status_code} (no such route)"
            if resp.status_code != 200:
                return False, None, f"HTTP {resp.status_code} {resp.text[:140]}"
            try:
                data = resp.json()
            except ValueError:
                return False, None, "response was not JSON"
            if isinstance(data, dict):
                data = data.get("presets") or data.get("data") or data.get("items") or []
            if not isinstance(data, list):
                return False, None, "no preset array in response"
            return True, data, ""

        template, payload, attempts = self._discover("presets", PRESET_LIST_ENDPOINTS, probe)
        if template is None:
            raise IikoError("no preset endpoint responded:\n  " + "\n  ".join(attempts))
        self.presets_endpoint = template
        return list(payload or [])

    def resolve_preset_id(self) -> Optional[str]:
        """Find the configured preset by id, else by (fuzzy) name."""
        if self.cfg.preset_id:
            return self.cfg.preset_id
        cached = _meta_get("preset_id")
        if cached:
            return cached
        want = _norm_key(self.cfg.preset_name)
        if not want:
            return None
        try:
            presets = self.list_presets()
        except Exception as exc:
            self._note(f"preset lookup unavailable: {exc}")
            return None
        for p in presets:
            name = str(p.get("name") or p.get("title") or p.get("presetName") or "")
            if _norm_key(name) == want:
                pid = str(p.get("id") or p.get("presetId") or p.get("uuid") or "")
                if pid:
                    _meta_set("preset_id", pid)
                    self._note(f"resolved preset '{name}' -> {pid}")
                    return pid
        # Fall back to a containment match ("Sales intr" vs "Sales intr v2").
        for p in presets:
            name = _norm_key(str(p.get("name") or p.get("title") or ""))
            if want and (want in name or name in want):
                pid = str(p.get("id") or p.get("presetId") or p.get("uuid") or "")
                if pid:
                    _meta_set("preset_id", pid)
                    return pid
        self._note(
            f"preset '{self.cfg.preset_name}' not found among "
            f"{len(presets)} presets; falling back to an explicit OLAP query"
        )
        return None

    # -- the actual report -------------------------------------------------
    def fetch_sales(self, date_from: dt.date, date_to: dt.date) -> pd.DataFrame:
        """Pull the Sales OLAP report for [date_from, date_to] inclusive."""
        self.authenticate()
        assert self.dialect

        frames: List[pd.DataFrame] = []
        for chunk_start, chunk_end in _chunk_range(date_from, date_to, self.cfg.chunk_days):
            rows = self._fetch_chunk(chunk_start, chunk_end)
            if rows:
                frames.append(pd.DataFrame(rows))
            # Be a polite API citizen on multi-month backfills.
            time.sleep(0.2)

        if not frames:
            return _empty_sales_frame()
        raw = pd.concat(frames, ignore_index=True)
        return normalise_olap_frame(raw)

    def _fetch_chunk(self, date_from: dt.date, date_to: dt.date) -> List[Dict[str, Any]]:
        assert self.dialect
        log_lines: List[str] = []

        # 1) Run the saved preset. This is the path the portal itself uses:
        #    POST /api/olap/fetch/{preset_id}/grouped-table
        preset_id = self.resolve_preset_id()
        if preset_id:
            params = {"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()}
            bodies = _preset_bodies(date_from, date_to)

            def preset_probe(path: str) -> Tuple[bool, Any, str]:
                # Builds disagree about where the date range belongs, so try each
                # shape before writing the path off. A 4xx that is not a routing
                # miss means the address is right and the payload is wrong —
                # keep trying payloads, then report the last real answer.
                last: Tuple[bool, Any, str] = (False, None, "no attempt made")
                for body in bodies:
                    resp = self._request("POST", path, json_body=body)
                    if resp.status_code not in _ROUTE_MISS:
                        last = _read_rows(resp)
                        if last[0]:
                            return last
                resp = self._request("POST", path, params=params, json_body={})
                if resp.status_code not in _ROUTE_MISS:
                    last = _read_rows(resp)
                    if last[0]:
                        return last
                resp = self._request("GET", path, params=params)
                if resp.status_code not in _ROUTE_MISS:
                    last = _read_rows(resp)
                    if last[0]:
                        return last
                    return last
                return False, None, f"HTTP {resp.status_code} (no such route)"

            template, rows, attempts = self._discover(
                "preset_run", PRESET_RUN_ENDPOINTS, preset_probe,
                format_args={"preset_id": preset_id},
            )
            if rows is not None:
                self.preset_run_endpoint = template
                log.info("iiko: %s..%s via preset %s -> %d rows",
                         date_from, date_to, template, len(rows))
                return rows
            log_lines.extend(f"preset {a}" for a in attempts)

        # 2) Fall back to an explicit SALES query. Preset-scoped candidates in
        #    this catalogue are templated with the id, or skipped without one.
        body = _olap_body(date_from, date_to, self.cfg.restaurant)

        def query_probe(path: str) -> Tuple[bool, Any, str]:
            return _read_rows(self._request("POST", path, json_body=body))

        template, rows, attempts = self._discover(
            "olap", OLAP_ENDPOINTS, query_probe,
            format_args={"preset_id": preset_id} if preset_id else None,
        )
        if rows is not None:
            self.olap_endpoint = template
            log.info("iiko: %s..%s via query %s -> %d rows",
                     date_from, date_to, template, len(rows))
            return rows
        log_lines.extend(f"query {a}" for a in attempts)

        raise IikoError(
            f"No OLAP endpoint answered for {date_from}..{date_to}. Tried:\n  "
            + "\n  ".join(log_lines)
            + "\n\nRun `python app.py doctor` to see the full probe. If a path"
            " answered 200 but no rows were recognised, the response shape is new:"
            " capture it from DevTools so _extract_rows can be taught to read it."
        )


def _preset_bodies(date_from: dt.date, date_to: dt.date) -> List[Dict[str, Any]]:
    """Candidate request bodies for a preset-scoped OLAP fetch.

    The portal's own payload was not captured, so the common shapes are tried in
    turn. Once the working one is known, delete the others.
    """
    f, t = date_from.isoformat(), date_to.isoformat()
    return [
        {"dateFrom": f, "dateTo": t},
        {"from": f, "to": t},
        {"startDate": f, "endDate": t},
        {"dateFrom": f, "dateTo": t, "buildSummary": False},
        {"period": {"from": f, "to": t}},
        {"filters": {"OpenDate.Typed": {"filterType": "DateRange",
                                        "periodType": "CUSTOM", "from": f, "to": t}}},
        {},
    ]


def _read_rows(resp: requests.Response) -> Tuple[bool, Optional[List[Dict[str, Any]]], str]:
    """Interpret an OLAP response as (routed_ok, rows, why-not)."""
    if resp.status_code in _ROUTE_MISS:
        return False, None, f"HTTP {resp.status_code} (no such route)"
    if resp.status_code != 200:
        return False, None, f"HTTP {resp.status_code} {resp.text[:140]}"
    try:
        data = resp.json()
    except ValueError:
        return False, None, "response was not JSON"
    rows = _extract_rows(data)
    if rows is None:
        return False, None, "no recognisable row array in response"
    return True, rows, ""


def _olap_body(date_from: dt.date, date_to: dt.date, restaurant: str = "") -> Dict[str, Any]:
    """Build an explicit SALES OLAP request (the preset-free fallback)."""
    filters: Dict[str, Any] = {
        "OpenDate.Typed": {
            "filterType": "DateRange",
            "periodType": "CUSTOM",
            "from": date_from.isoformat(),
            "to": date_to.isoformat(),
            "includeLow": True,
            "includeHigh": True,
        },
        "DeletedWithWriteoff": {
            "filterType": "IncludeValues",
            "values": ["NOT_DELETED"],
        },
        "OrderDeleted": {
            "filterType": "IncludeValues",
            "values": ["NOT_DELETED"],
        },
    }
    return {
        "reportType": "SALES",
        "buildSummary": False,
        "groupByRowFields": list(OLAP_ROW_FIELDS),
        "groupByColFields": [],
        "aggregateFields": list(OLAP_AGG_FIELDS),
        "filters": filters,
    }


_COLUMN_KEYS = ("columns", "header", "headers", "fields", "columnNames", "cols")
_ROW_KEYS = ("rows", "data", "result", "items", "report", "values", "records", "table")
_CHILD_KEYS = ("children", "groups", "subRows", "items", "rows", "nodes")


def _column_names(spec: Any) -> Optional[List[str]]:
    """Read a column list that may be plain strings or descriptor objects."""
    if not isinstance(spec, list) or not spec:
        return None
    names: List[str] = []
    for col in spec:
        if isinstance(col, str):
            names.append(col)
        elif isinstance(col, dict):
            name = (col.get("name") or col.get("title") or col.get("field")
                    or col.get("id") or col.get("key") or col.get("header"))
            if not name:
                return None
            names.append(str(name))
        else:
            return None
    return names


def _rows_from_table(data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Zip a columns + array-of-arrays table into dicts.

    The portal's `grouped-table` route returns a table, not a list of objects,
    so the header has to be matched to positional cells.
    """
    columns = None
    for key in _COLUMN_KEYS:
        columns = _column_names(data.get(key))
        if columns:
            break
    if not columns:
        return None
    for key in _ROW_KEYS:
        rows = data.get(key)
        if not isinstance(rows, list) or not rows:
            continue
        if all(isinstance(r, (list, tuple)) for r in rows):
            return [
                {columns[i]: cell for i, cell in enumerate(r) if i < len(columns)}
                for r in rows
            ]
    return None


def _flatten_groups(node: Any, inherited: Optional[Dict[str, Any]] = None,
                    depth: int = 0) -> List[Dict[str, Any]]:
    """Flatten a grouped tree into leaf rows, carrying each group's own fields.

    A grouped report nests rows under the dimensions it is grouped by. The leaf
    holds the measures; the ancestors hold the dimension values, so the group
    fields have to be pushed down or the date and tender would be lost.
    """
    inherited = dict(inherited or {})
    out: List[Dict[str, Any]] = []

    if isinstance(node, list):
        for item in node:
            out.extend(_flatten_groups(item, inherited, depth))
        return out
    if not isinstance(node, dict):
        return out

    children = None
    for key in _CHILD_KEYS:
        val = node.get(key)
        if isinstance(val, list) and val and isinstance(val[0], (dict, list)):
            children = val
            break

    own = {k: v for k, v in node.items()
           if not isinstance(v, (dict, list)) and k not in ("id", "uuid")}
    merged = {**inherited, **own}

    if children is None:
        return [merged] if merged else []
    if depth > 8:                                   # pathological nesting guard
        return [merged] if merged else []
    for child in children:
        out.extend(_flatten_groups(child, merged, depth + 1))
    return out


def _extract_rows(data: Any) -> Optional[List[Dict[str, Any]]]:
    """Locate the row array inside any of the shapes iiko returns."""
    if isinstance(data, list):
        if all(isinstance(x, dict) for x in data):
            # A list of dicts may still be a grouped tree rather than flat rows.
            if any(any(isinstance(x.get(k), list) for k in _CHILD_KEYS) for x in data):
                flat = _flatten_groups(data)
                if flat:
                    return flat
            return data
        return None
    if not isinstance(data, dict):
        return None

    # columns + array-of-arrays (the `grouped-table` shape)
    table = _rows_from_table(data)
    if table is not None:
        return table

    for key in _ROW_KEYS:
        val = data.get(key)
        if isinstance(val, list) and (not val or isinstance(val[0], dict)):
            if val and any(any(isinstance(x.get(k), list) for k in _CHILD_KEYS)
                           for x in val if isinstance(x, dict)):
                flat = _flatten_groups(val)
                if flat:
                    return flat
            return val
        if isinstance(val, dict):
            nested = _extract_rows(val)
            if nested is not None:
                return nested

    # A grouped tree hanging off an unconventional key.
    for key in _CHILD_KEYS:
        val = data.get(key)
        if isinstance(val, list) and val:
            flat = _flatten_groups(val)
            if flat:
                return flat
    return None


def _dig(obj: Any, keys: Sequence[str]) -> Optional[Any]:
    """Depth-first search for the first of `keys` present in a nested dict."""
    if isinstance(obj, dict):
        for k in keys:
            if obj.get(k):
                return obj[k]
        for v in obj.values():
            found = _dig(v, keys)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _dig(v, keys)
            if found:
                return found
    return None


def _chunk_range(start: dt.date, end: dt.date, size_days: int) -> Iterable[Tuple[dt.date, dt.date]]:
    if end < start:
        start, end = end, start
    size_days = max(1, int(size_days))
    cur = start
    while cur <= end:
        stop = min(cur + dt.timedelta(days=size_days - 1), end)
        yield cur, stop
        cur = stop + dt.timedelta(days=1)


# ==============================================================================
#  SECTION 5 — NORMALISATION
# ==============================================================================

CANON_COLUMNS = ["business_date", "payment_type", "order_count", "gross_sales", "net_sales", "cogs"]


def _empty_sales_frame() -> pd.DataFrame:
    df = pd.DataFrame(columns=CANON_COLUMNS + ["channel"])
    df["business_date"] = pd.to_datetime(df["business_date"]).dt.date
    return df


def _to_number(val: Any) -> float:
    """Coerce iiko's assorted numeric renderings into a float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    s = s.replace(" ", "").replace(" ", "").replace(CURRENCY_SYMBOL, "")
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and "." in s:
        s = s.replace(",", "") if s.rfind(".") > s.rfind(",") else s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_date(val: Any) -> Optional[dt.date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, dt.datetime):
        return val.date()
    if isinstance(val, dt.date):
        return val
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("T", " ").split(" ")[0]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(parsed) else parsed.date()


def _guess_date_column(raw: pd.DataFrame, exclude: Iterable[str] = ()) -> Optional[str]:
    """Find the column that behaves like a business date."""
    best, best_rate = None, 0.0
    sample = raw.head(200)
    for col in raw.columns:
        if col in set(exclude):
            continue
        vals = [v for v in sample[col].tolist() if v not in (None, "")]
        if not vals:
            continue
        hits = sum(1 for v in vals if _to_date(v) is not None)
        rate = hits / len(vals)
        # A pure integer column (order counts) can parse as a date; require the
        # values to actually look like dates rather than bare numbers.
        if rate > best_rate and rate >= 0.8 and any(re.search(r"[-/.]", str(v)) for v in vals[:20]):
            best, best_rate = col, rate
    return best


def _guess_payment_column(raw: pd.DataFrame, exclude: Iterable[str] = ()) -> Optional[str]:
    """Find the column that behaves like a tender / payment-type dimension."""
    ex = set(exclude)
    sample = raw.head(200)
    best, best_score = None, 0.0
    for col in raw.columns:
        if col in ex:
            continue
        vals = [str(v) for v in sample[col].tolist() if v not in (None, "")]
        if not vals:
            continue
        # Mostly non-numeric text, with a small number of repeating values.
        texty = sum(1 for v in vals if not re.fullmatch(r"[\d\s.,\-]+", v)) / len(vals)
        distinct = len(set(vals))
        if texty < 0.7 or distinct > 60:
            continue
        # Prefer a column whose values the rule engine actually recognises.
        recognised = sum(1 for v in set(vals) if classify_channel(v) != "Hall")
        score = texty + min(recognised, 4) * 0.25
        if score > best_score:
            best, best_score = col, score
    return best


def normalise_olap_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Translate raw Russian OLAP output into the canonical English schema."""
    if raw is None or raw.empty:
        return _empty_sales_frame()

    rename: Dict[str, str] = {}
    for col in raw.columns:
        canon = FIELD_ALIASES.get(_norm_key(col))
        if canon and canon not in rename.values():
            rename[col] = canon

    # The alias table covers the documented preset, but a saved OLAP view can be
    # renamed in iiko at any time. Rather than silently returning an empty
    # report, sniff the two dimensions we cannot do without.
    if "business_date" not in rename.values():
        guess = _guess_date_column(raw, exclude=set(rename))
        if guess:
            rename[guess] = "business_date"
            log.warning("normalise: no known date header; inferred '%s'", guess)
    if "payment_type" not in rename.values():
        guess = _guess_payment_column(raw, exclude=set(rename))
        if guess:
            rename[guess] = "payment_type"
            log.warning("normalise: no known tender header; inferred '%s'", guess)
    df = raw.rename(columns=rename).copy()

    # Anything the alias table missed is dropped, but logged once so an
    # unexpected preset column is visible rather than silently discarded.
    unknown = [c for c in df.columns if c not in set(FIELD_ALIASES.values())]
    if unknown:
        log.debug("normalise: ignoring unmapped columns %s", unknown[:12])

    for col in CANON_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["business_date"] = df["business_date"].map(_to_date)
    df["payment_type"] = df["payment_type"].fillna("").astype(str).str.strip()
    for col in ("order_count", "gross_sales", "net_sales", "cogs"):
        df[col] = df[col].map(_to_number)

    # Some presets expose only net sales; treat gross as net when absent so
    # discount metrics degrade to zero instead of producing negative revenue.
    df.loc[df["gross_sales"] <= 0, "gross_sales"] = df["net_sales"]

    df["order_count"] = df["order_count"].round().astype("int64")
    df["channel"] = df["payment_type"].map(classify_channel)
    df = df[df["business_date"].notna()].copy()

    out = (
        df.groupby(["business_date", "payment_type", "channel"], as_index=False)[
            ["order_count", "gross_sales", "net_sales", "cogs"]
        ].sum()
    )
    return out[["business_date", "payment_type", "channel", "order_count", "gross_sales", "net_sales", "cogs"]]


# ==============================================================================
#  SECTION 6 — LOCAL CACHE (SQLite)
# ==============================================================================
#  The dashboard never queries iiko on user interaction. A background sync keeps
#  a local SQLite mirror of the year-to-date Sales OLAP report, and every filter,
#  comparison and export is served from that mirror. This keeps the UI instant
#  and the API load to one small request per morning.

SCHEMA = """
CREATE TABLE IF NOT EXISTS sales_daily (
    business_date TEXT    NOT NULL,
    payment_type  TEXT    NOT NULL,
    channel       TEXT    NOT NULL,
    order_count   INTEGER NOT NULL DEFAULT 0,
    gross_sales   REAL    NOT NULL DEFAULT 0,
    net_sales     REAL    NOT NULL DEFAULT 0,
    cogs          REAL    NOT NULL DEFAULT 0,
    PRIMARY KEY (business_date, payment_type)
);
CREATE INDEX IF NOT EXISTS ix_sales_date    ON sales_daily (business_date);
CREATE INDEX IF NOT EXISTS ix_sales_channel ON sales_daily (channel);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

_db_lock = threading.Lock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db_lock, _connect() as conn:
        conn.executescript(SCHEMA)


def _meta_get(key: str) -> Optional[str]:
    try:
        with _db_lock, _connect() as conn:
            conn.executescript(SCHEMA)
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None
    except Exception:
        return None


def _meta_set(key: str, value: str) -> None:
    try:
        with _db_lock, _connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
    except Exception as exc:
        log.warning("meta write failed for %s: %s", key, exc)


def upsert_sales(df: pd.DataFrame, replace_range: Optional[Tuple[dt.date, dt.date]] = None) -> int:
    """Write rows into the mirror, optionally clearing a date range first.

    Clearing is what makes a re-pull authoritative: if a check was voided in
    iiko after the fact, the stale row disappears instead of lingering.
    """
    init_db()
    if df is None:
        df = _empty_sales_frame()

    with _db_lock, _connect() as conn:
        if replace_range:
            lo, hi = replace_range
            conn.execute(
                "DELETE FROM sales_daily WHERE business_date BETWEEN ? AND ?",
                (lo.isoformat(), hi.isoformat()),
            )
        if df.empty:
            return 0
        payload = [
            (
                r.business_date.isoformat() if isinstance(r.business_date, dt.date) else str(r.business_date),
                str(r.payment_type),
                str(r.channel),
                int(r.order_count),
                float(r.gross_sales),
                float(r.net_sales),
                float(r.cogs),
            )
            for r in df.itertuples(index=False)
        ]
        conn.executemany(
            "INSERT INTO sales_daily "
            "(business_date, payment_type, channel, order_count, gross_sales, net_sales, cogs) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(business_date, payment_type) DO UPDATE SET "
            "  channel     = excluded.channel,"
            "  order_count = excluded.order_count,"
            "  gross_sales = excluded.gross_sales,"
            "  net_sales   = excluded.net_sales,"
            "  cogs        = excluded.cogs",
            payload,
        )
        return len(payload)


def load_sales(date_from: Optional[dt.date] = None, date_to: Optional[dt.date] = None) -> pd.DataFrame:
    init_db()
    q = "SELECT * FROM sales_daily"
    params: List[str] = []
    if date_from and date_to:
        q += " WHERE business_date BETWEEN ? AND ?"
        params = [date_from.isoformat(), date_to.isoformat()]
    q += " ORDER BY business_date"
    with _db_lock, _connect() as conn:
        df = pd.read_sql_query(q, conn, params=params)
    if df.empty:
        return _empty_sales_frame()
    df["business_date"] = pd.to_datetime(df["business_date"]).dt.date
    # Re-derive the channel on read so a change to the rule engine takes effect
    # immediately, without needing the cache rebuilt.
    df["channel"] = df["payment_type"].map(classify_channel)
    return df


def cache_span() -> Tuple[Optional[dt.date], Optional[dt.date], int]:
    init_db()
    with _db_lock, _connect() as conn:
        row = conn.execute(
            "SELECT MIN(business_date) lo, MAX(business_date) hi, COUNT(*) n FROM sales_daily"
        ).fetchone()
    if not row or not row["lo"]:
        return None, None, 0
    return _to_date(row["lo"]), _to_date(row["hi"]), int(row["n"])


# ==============================================================================
#  SECTION 7 — SYNCHRONISATION
# ==============================================================================

def sync(full: bool = False, client: Optional[IikoClient] = None) -> Dict[str, Any]:
    """Refresh the local mirror. Returns a small report for the UI / CLI."""
    init_db()
    client = client or IikoClient()
    today = dt.date.today()
    lo, hi, _ = cache_span()

    if full or lo is None:
        start = dt.date(today.year, 1, 1)
        mode = "full year-to-date"
    else:
        tail = dt.timedelta(days=max(0, CFG.incremental_tail_days))
        start = min(hi, today) - tail
        start = max(start, dt.date(today.year, 1, 1))
        mode = f"incremental (last {CFG.incremental_tail_days} days)"

    t0 = time.time()
    # Stamped on every sync so a log file proves which build produced it.
    log.info("nani dashboard v%s — sync start (%s), OLAP order leads with %s",
             APP_VERSION, mode,
             _ordered_candidates(CFG.olap_endpoint, _meta_get("endpoint_olap"), OLAP_ENDPOINTS)[0])
    result: Dict[str, Any] = {"mode": mode, "from": start, "to": today, "rows": 0, "ok": False}
    try:
        df = client.fetch_sales(start, today)
        written = upsert_sales(df, replace_range=(start, today))
        _meta_set("last_sync_at", dt.datetime.now().isoformat(timespec="seconds"))
        _meta_set("last_sync_date", today.isoformat())
        _meta_set("last_sync_mode", mode)
        _meta_set("last_sync_error", "")
        result.update(rows=written, ok=True, seconds=round(time.time() - t0, 1))
        log.info("sync %s: %s..%s -> %d rows in %.1fs", mode, start, today, written, time.time() - t0)
    except Exception as exc:
        _meta_set("last_sync_error", str(exc)[:500])
        result["error"] = str(exc)
        log.error("sync failed: %s", exc)
    finally:
        with contextlib.suppress(Exception):
            client.logout()
    return result


def sync_if_stale() -> Optional[Dict[str, Any]]:
    """Called on every app load: pull once per calendar day, in the morning."""
    last = _meta_get("last_sync_date")
    today = dt.date.today().isoformat()
    if last == today:
        return None
    lo, _, _ = cache_span()
    return sync(full=(lo is None))


# ==============================================================================
#  SECTION 8 — METRICS
# ==============================================================================

@dataclass
class Metrics:
    net_sales: float = 0.0
    gross_sales: float = 0.0
    order_count: int = 0
    cogs: float = 0.0

    @property
    def avg_check(self) -> float:
        return self.net_sales / self.order_count if self.order_count else 0.0

    @property
    def cogs_pct(self) -> float:
        return self.cogs / self.net_sales if self.net_sales else 0.0

    @property
    def discounts(self) -> float:
        return self.gross_sales - self.net_sales

    @property
    def discount_pct(self) -> float:
        return (1 - self.net_sales / self.gross_sales) if self.gross_sales else 0.0

    @property
    def gross_profit(self) -> float:
        return self.net_sales - self.cogs

    @property
    def gross_margin_pct(self) -> float:
        return self.gross_profit / self.net_sales if self.net_sales else 0.0


def compute_metrics(df: pd.DataFrame) -> Metrics:
    if df is None or df.empty:
        return Metrics()
    return Metrics(
        net_sales=float(df["net_sales"].sum()),
        gross_sales=float(df["gross_sales"].sum()),
        order_count=int(df["order_count"].sum()),
        cogs=float(df["cogs"].sum()),
    )


def slice_period(df: pd.DataFrame, lo: dt.date, hi: dt.date, channel: Optional[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_sales_frame()
    mask = (df["business_date"] >= lo) & (df["business_date"] <= hi)
    out = df.loc[mask]
    if channel:
        out = out.loc[out["channel"] == channel]
    return out.copy()


def revenue_only(df: pd.DataFrame) -> pd.DataFrame:
    """Compliments are a cost, not a sale — strip them from revenue views."""
    if df is None or df.empty:
        return df
    return df.loc[df["channel"] != "Compliments"].copy()


# ==============================================================================
#  SECTION 9 — PERIOD & COMPARISON LOGIC
# ==============================================================================

def snap_to_full_weeks(lo: dt.date, hi: dt.date, week_start: int = 0) -> Tuple[dt.date, dt.date]:
    """Expand a range outwards to whole calendar weeks.

    Comparing a 45-day window against the previous 45 days silently compares
    six Saturdays with five — which for a restaurant is a large distortion.
    Snapping to whole weeks guarantees both windows hold identical day-of-week
    composition. `week_start` is 0 for Monday, 6 for Sunday.
    """
    back = (lo.weekday() - week_start) % 7
    fwd = (week_start - 1 - hi.weekday()) % 7
    return lo - dt.timedelta(days=back), hi + dt.timedelta(days=fwd)


def previous_period(lo: dt.date, hi: dt.date, mode: str = "auto") -> Tuple[dt.date, dt.date]:
    """The comparison window immediately preceding [lo, hi]."""
    if mode == "month":
        prev_end = lo - dt.timedelta(days=1)
        prev_start = dt.date(prev_end.year, prev_end.month, 1)
        return prev_start, prev_end
    if mode == "year":
        with contextlib.suppress(ValueError):
            return lo.replace(year=lo.year - 1), hi.replace(year=hi.year - 1)
    span = (hi - lo).days + 1
    return lo - dt.timedelta(days=span), lo - dt.timedelta(days=1)


def month_bounds(anchor: dt.date) -> Tuple[dt.date, dt.date]:
    start = anchor.replace(day=1)
    nxt = (start + dt.timedelta(days=32)).replace(day=1)
    return start, nxt - dt.timedelta(days=1)


def build_period_presets(today: dt.date) -> Dict[str, Tuple[dt.date, dt.date, str]]:
    """label -> (from, to, comparison_mode)"""
    week_start = today - dt.timedelta(days=today.weekday())
    last_week_start = week_start - dt.timedelta(days=7)
    m_start, m_end = month_bounds(today)
    prev_month_anchor = m_start - dt.timedelta(days=1)
    pm_start, pm_end = month_bounds(prev_month_anchor)
    return {
        "Today": (today, today, "auto"),
        "Yesterday": (today - dt.timedelta(days=1), today - dt.timedelta(days=1), "auto"),
        "Last 7 days": (today - dt.timedelta(days=6), today, "auto"),
        "This week": (week_start, today, "auto"),
        "Last week": (last_week_start, last_week_start + dt.timedelta(days=6), "auto"),
        "Last 30 days": (today - dt.timedelta(days=29), today, "auto"),
        "This month": (m_start, min(m_end, today), "month"),
        "Last month": (pm_start, pm_end, "month"),
        "Quarter to date": (dt.date(today.year, 3 * ((today.month - 1) // 3) + 1, 1), today, "auto"),
        "Year to date": (dt.date(today.year, 1, 1), today, "year"),
        "Custom range": (today - dt.timedelta(days=27), today, "auto"),
    }


GRAIN_LABELS = ["Daily", "Weekly", "Monthly"]


def add_period_bucket(df: pd.DataFrame, grain: str) -> pd.DataFrame:
    """Attach a `period` column (a date) and `period_label` (display text)."""
    out = df.copy()
    if out.empty:
        out["period"] = []
        out["period_label"] = []
        return out
    ds = pd.to_datetime(out["business_date"])
    if grain == "Weekly":
        starts = ds - pd.to_timedelta(ds.dt.weekday, unit="D")
        out["period"] = starts.dt.date
        out["period_label"] = starts.dt.strftime("W%V · %d %b") + (starts + pd.Timedelta(days=6)).dt.strftime(" – %d %b %Y")
    elif grain == "Monthly":
        starts = ds.dt.to_period("M").dt.to_timestamp()
        out["period"] = starts.dt.date
        out["period_label"] = starts.dt.strftime("%B %Y")
    else:
        out["period"] = ds.dt.date
        out["period_label"] = ds.dt.strftime("%a, %d %b %Y")
    return out


def drilldown_table(df: pd.DataFrame, grain: str) -> pd.DataFrame:
    """The bottom-of-page summary table, in presentation order."""
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["Period", "Gross Sales", "Net Sales", "Orders", "Average Check", "COGS", "COGS %"]
        )
    b = add_period_bucket(df, grain)
    g = (
        b.groupby(["period", "period_label"], as_index=False)[
            ["gross_sales", "net_sales", "order_count", "cogs"]
        ]
        .sum()
        .sort_values("period", ascending=False)
    )
    g["avg_check"] = g.apply(lambda r: r.net_sales / r.order_count if r.order_count else 0.0, axis=1)
    g["cogs_pct"] = g.apply(lambda r: r.cogs / r.net_sales if r.net_sales else 0.0, axis=1)
    out = g[["period_label", "gross_sales", "net_sales", "order_count", "avg_check", "cogs", "cogs_pct"]]
    out.columns = ["Period", "Gross Sales", "Net Sales", "Orders", "Average Check", "COGS", "COGS %"]
    return out.reset_index(drop=True)


# ==============================================================================
#  SECTION 10 — FORMATTING & EXCEL EXPORT
# ==============================================================================

def fmt_money(v: float, decimals: int = 0) -> str:
    try:
        return f"{v:,.{decimals}f} {CURRENCY_SYMBOL}"
    except (TypeError, ValueError):
        return "—"


def fmt_int(v: float) -> str:
    try:
        return f"{int(round(v)):,}"
    except (TypeError, ValueError):
        return "—"


def fmt_pct(v: float, decimals: int = 1) -> str:
    try:
        return f"{v * 100:,.{decimals}f}%"
    except (TypeError, ValueError):
        return "—"


def fmt_compact(v: float) -> str:
    """Large currency figures, shortened for KPI tiles."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    a = abs(v)
    if a >= 1_000_000_000:
        return f"{v/1_000_000_000:,.2f}B {CURRENCY_SYMBOL}"
    if a >= 1_000_000:
        return f"{v/1_000_000:,.2f}M {CURRENCY_SYMBOL}"
    if a >= 10_000:
        return f"{v/1_000:,.1f}K {CURRENCY_SYMBOL}"
    return f"{v:,.0f} {CURRENCY_SYMBOL}"


def delta_parts(cur: float, prev: float) -> Tuple[str, str, str]:
    """(absolute text, percent text, direction) for a comparison badge."""
    diff = cur - prev
    if prev == 0:
        pct_txt = "n/a" if diff == 0 else "new"
    else:
        pct_txt = f"{diff / abs(prev) * 100:+,.1f}%"
    direction = "flat" if abs(diff) < 1e-9 else ("up" if diff > 0 else "down")
    return f"{diff:+,.0f}", pct_txt, direction


def build_excel(
    table: pd.DataFrame,
    metrics: Metrics,
    title: str,
    period_text: str,
    channel_mix: Optional[pd.DataFrame] = None,
) -> bytes:
    """Render a formatted, CFO-ready .xlsx workbook."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
        book = xw.book

        f_title = book.add_format({"bold": True, "font_size": 16, "font_color": "#0F172A"})
        f_sub = book.add_format({"font_size": 10, "font_color": "#64748B", "italic": True})
        f_head = book.add_format({
            "bold": True, "font_color": "#F1F5F9", "bg_color": "#1E293B",
            "border": 1, "border_color": "#334155", "align": "center", "valign": "vcenter", "text_wrap": True,
        })
        f_txt = book.add_format({"border": 1, "border_color": "#E2E8F0"})
        f_money = book.add_format({"num_format": '#,##0 "֏"', "border": 1, "border_color": "#E2E8F0"})
        f_int = book.add_format({"num_format": "#,##0", "border": 1, "border_color": "#E2E8F0"})
        f_pct = book.add_format({"num_format": "0.0%", "border": 1, "border_color": "#E2E8F0"})
        f_klabel = book.add_format({"bold": True, "font_color": "#334155"})
        f_kmoney = book.add_format({"num_format": '#,##0 "֏"', "bold": True, "font_size": 12})
        f_kint = book.add_format({"num_format": "#,##0", "bold": True, "font_size": 12})
        f_kpct = book.add_format({"num_format": "0.0%", "bold": True, "font_size": 12})

        # ---- Summary sheet ------------------------------------------------
        ws = book.add_worksheet("Summary")
        ws.hide_gridlines(2)
        ws.set_column("A:A", 30)
        ws.set_column("B:B", 22)
        ws.write("A1", f"{CFG.restaurant} — {title}", f_title)
        ws.write("A2", period_text, f_sub)

        kpis: List[Tuple[str, float, Any]] = [
            ("Gross Sales (before discount)", metrics.gross_sales, f_kmoney),
            ("Net Sales (after discount)", metrics.net_sales, f_kmoney),
            ("Discounts", metrics.discounts, f_kmoney),
            ("Discount %", metrics.discount_pct, f_kpct),
            ("Order Count (checks)", metrics.order_count, f_kint),
            ("Average Check", metrics.avg_check, f_kmoney),
            ("COGS", metrics.cogs, f_kmoney),
            ("COGS %", metrics.cogs_pct, f_kpct),
            ("Gross Profit", metrics.gross_profit, f_kmoney),
            ("Gross Margin %", metrics.gross_margin_pct, f_kpct),
        ]
        row = 4
        for label, value, fmt in kpis:
            ws.write(row, 0, label, f_klabel)
            ws.write_number(row, 1, float(value), fmt)
            row += 1

        if channel_mix is not None and not channel_mix.empty:
            row += 2
            ws.write(row, 0, "Channel mix", f_klabel)
            row += 1
            ws.write_row(row, 0, ["Channel", "Net Sales", "Share"], f_head)
            total = float(channel_mix["net_sales"].sum()) or 1.0
            for _, r in channel_mix.iterrows():
                row += 1
                ws.write(row, 0, str(r["channel"]), f_txt)
                ws.write_number(row, 1, float(r["net_sales"]), f_money)
                ws.write_number(row, 2, float(r["net_sales"]) / total, f_pct)

        # ---- Detail sheet -------------------------------------------------
        ws2 = book.add_worksheet("Detail")
        ws2.hide_gridlines(2)
        ws2.freeze_panes(1, 1)
        headers = list(table.columns)
        widths = [30, 18, 18, 12, 16, 18, 10]
        for i, (h, w) in enumerate(zip(headers, widths + [16] * len(headers))):
            ws2.write(0, i, h, f_head)
            ws2.set_column(i, i, w)
        fmt_by_col = {
            "Period": f_txt, "Gross Sales": f_money, "Net Sales": f_money,
            "Orders": f_int, "Average Check": f_money, "COGS": f_money, "COGS %": f_pct,
        }
        for r_i, (_, r) in enumerate(table.iterrows(), start=1):
            for c_i, col in enumerate(headers):
                val = r[col]
                fmt = fmt_by_col.get(col, f_txt)
                if isinstance(val, (int, float)) and not isinstance(val, bool):
                    ws2.write_number(r_i, c_i, float(val), fmt)
                else:
                    ws2.write(r_i, c_i, str(val), fmt)
        if len(table):
            ws2.autofilter(0, 0, len(table), len(headers) - 1)

    buf.seek(0)
    return buf.read()


# ==============================================================================
#  SECTION 11 — STYLESHEET
# ==============================================================================
#  Streamlit's default chrome is not an executive surface. The sheet below
#  rebuilds the page as a dark, quiet, print-adjacent CFO console: generous
#  whitespace, one accent colour, and typography that survives a phone screen.

CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=Playfair+Display:wght@600;700&family=Noto+Sans+Armenian:wght@400;600;700&display=swap');

  :root {
    --obsidian:  #0F172A;
    --slate:     #1E293B;
    --slate-2:   #273449;
    --hairline:  #334155;
    --gold:      #D4AF37;
    --gold-soft: #E8CE73;
    --emerald:   #10B981;
    --crimson:   #EF4444;
    --text:      #F1F5F9;
    --muted:     #94A3B8;
    --faint:     #64748B;
  }

  .stApp {
    background:
      radial-gradient(1200px 600px at 15% -10%, #16233d 0%, rgba(15,23,42,0) 60%),
      var(--obsidian);
    color: var(--text);
    /* Noto Sans Armenian carries the dram sign (֏), which most UI fonts lack. */
    font-family: 'Inter', 'Noto Sans Armenian', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  }
  .block-container { padding: .6rem 1.6rem 4rem; max-width: 1560px; }
  #MainMenu, footer, header [data-testid="stToolbar"] { visibility: hidden; }
  header[data-testid="stHeader"] { background: transparent; height: 0; }
  [data-testid="stDecoration"] { display: none; }

  h1, h2, h3, h4 { color: var(--text); font-weight: 700; letter-spacing: -0.015em; }

  /* ---------- Masthead ---------- */
  .nani-head {
    display: flex; align-items: center; gap: 18px; flex-wrap: wrap;
    padding: 16px 20px; margin-bottom: 18px;
    background: linear-gradient(135deg, rgba(30,41,59,.95) 0%, rgba(15,23,42,.75) 100%);
    border: 1px solid var(--hairline);
    border-left: 3px solid var(--gold);
    border-radius: 14px;
  }
  .nani-head img { height: 52px; width: auto; border-radius: 10px; display: block; }
  .nani-head .title {
    font-family: 'Playfair Display', Georgia, serif;
    font-size: 1.6rem; font-weight: 700; color: var(--text); line-height: 1.15; margin: 0;
  }
  .nani-head .subtitle {
    font-size: .74rem; color: var(--muted); letter-spacing: .16em;
    text-transform: uppercase; margin-top: 4px;
  }
  .nani-head .spacer { flex: 1 1 auto; }
  .nani-head .stamp { text-align: right; font-size: .72rem; color: var(--faint); line-height: 1.6; }
  .nani-head .stamp b { color: var(--gold-soft); font-weight: 600; }

  /* ---------- KPI tiles ---------- */
  /* The column count is declared per row rather than left to auto-fit, so a
     six-tile row never breaks as five-plus-one orphan. */
  .kpi-grid {
    display: grid; gap: 12px; margin: 6px 0 20px;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  }
  .kpi-grid.cols-6 { grid-template-columns: repeat(6, minmax(0, 1fr)); }
  .kpi-grid.cols-5 { grid-template-columns: repeat(5, minmax(0, 1fr)); }
  .kpi-grid.cols-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .kpi-grid.cols-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .kpi-grid.cols-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .kpi {
    background: linear-gradient(160deg, var(--slate) 0%, #1a2436 100%);
    border: 1px solid var(--hairline);
    border-radius: 14px; padding: 16px 18px 14px;
    position: relative; overflow: hidden;
    transition: transform .18s ease, border-color .18s ease;
  }
  .kpi:hover { transform: translateY(-2px); border-color: var(--slate-2); }
  .kpi::after {
    content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 3px;
    background: var(--gold); opacity: .8;
  }
  .kpi.accent-emerald::after { background: var(--emerald); }
  .kpi.accent-crimson::after { background: var(--crimson); }
  .kpi.accent-muted::after   { background: var(--faint); }

  .kpi .label {
    font-size: .68rem; text-transform: uppercase; letter-spacing: .14em;
    color: var(--muted); font-weight: 600; margin-bottom: 8px;
  }
  .kpi .value {
    font-size: 1.62rem; font-weight: 700; color: var(--text);
    line-height: 1.12; letter-spacing: -0.02em; font-variant-numeric: tabular-nums;
  }
  .kpi .value.sm { font-size: 1.32rem; }
  .kpi .sub { font-size: .7rem; color: var(--faint); margin-top: 6px; }

  .badge {
    display: inline-flex; align-items: center; gap: 5px; margin-top: 10px;
    padding: 3px 9px; border-radius: 999px;
    font-size: .72rem; font-weight: 600; font-variant-numeric: tabular-nums;
  }
  .badge.up   { background: rgba(16,185,129,.14); color: var(--emerald); border: 1px solid rgba(16,185,129,.3); }
  .badge.down { background: rgba(239,68,68,.14);  color: var(--crimson); border: 1px solid rgba(239,68,68,.3); }
  .badge.flat { background: rgba(148,163,184,.12); color: var(--muted);  border: 1px solid rgba(148,163,184,.25); }
  .badge .abs { color: var(--faint); font-weight: 500; }

  /* ---------- Section furniture ---------- */
  .sec-title {
    font-size: .74rem; text-transform: uppercase; letter-spacing: .18em;
    color: var(--gold); font-weight: 700; margin: 26px 0 10px;
    padding-bottom: 8px; border-bottom: 1px solid var(--hairline);
  }
  .panel {
    background: var(--slate); border: 1px solid var(--hairline);
    border-radius: 14px; padding: 16px 18px; margin-bottom: 14px;
  }
  .note { font-size: .74rem; color: var(--faint); line-height: 1.6; }
  .chip {
    display: inline-block; padding: 3px 10px; border-radius: 999px;
    background: rgba(212,175,55,.12); border: 1px solid rgba(212,175,55,.3);
    color: var(--gold-soft); font-size: .7rem; font-weight: 600; margin-right: 6px;
  }

  /* ---------- Channel header ---------- */
  .ch-head { display: flex; align-items: center; gap: 14px; margin-bottom: 4px; }
  .ch-head img { height: 44px; width: auto; border-radius: 9px; }
  .ch-head .name { font-size: 1.22rem; font-weight: 700; }
  .ch-head .dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }

  /* ---------- Streamlit control overrides ---------- */
  section[data-testid="stSidebar"] {
    background: #0c1424; border-right: 1px solid var(--hairline);
  }
  section[data-testid="stSidebar"] .block-container { padding-top: 1.2rem; }

  .stTabs [data-baseweb="tab-list"] {
    gap: 4px; background: transparent; border-bottom: 1px solid var(--hairline);
  }
  .stTabs [data-baseweb="tab"] {
    height: 42px; padding: 0 16px; background: transparent;
    color: var(--muted); font-weight: 600; font-size: .86rem;
    border-radius: 9px 9px 0 0;
  }
  .stTabs [aria-selected="true"] {
    background: var(--slate) !important; color: var(--gold) !important;
    border: 1px solid var(--hairline); border-bottom: 1px solid var(--slate);
  }

  div[data-testid="stDataFrame"] { border: 1px solid var(--hairline); border-radius: 12px; }

  /* Widget surfaces. Streamlit themes these from config.toml, which the app
     writes on first run; these rules keep the console coherent even on the
     very first launch, before that config is picked up. */
  div[data-baseweb="select"] > div,
  div[data-baseweb="input"]  > div,
  div[data-testid="stDateInput"] input {
    background-color: #16223a !important;
    border-color: var(--hairline) !important;
    color: var(--text) !important;
  }
  div[data-baseweb="select"] svg { fill: var(--muted); }
  ul[data-baseweb="menu"], div[data-baseweb="popover"] > div {
    background-color: #16223a !important; color: var(--text) !important;
  }
  li[data-baseweb="menu-item"]:hover { background-color: #22304b !important; }
  div[data-testid="stExpander"] details {
    background: var(--slate); border: 1px solid var(--hairline); border-radius: 10px;
  }
  div[data-testid="stExpander"] summary { color: var(--muted); }

  /* ---------- Drill-down table ----------
     Rendered as plain HTML rather than st.dataframe so it inherits the
     executive palette exactly and scrolls horizontally on a phone. */
  .tbl-wrap {
    overflow: auto; max-height: 520px;
    border: 1px solid var(--hairline); border-radius: 12px;
    background: var(--slate); margin-bottom: 12px;
    -webkit-overflow-scrolling: touch;
  }
  table.drill { width: 100%; border-collapse: collapse; font-size: .82rem; }
  table.drill thead th {
    position: sticky; top: 0; z-index: 2;
    background: #16223a; color: var(--muted);
    font-size: .66rem; text-transform: uppercase; letter-spacing: .1em; font-weight: 700;
    padding: 11px 14px; text-align: right; white-space: nowrap;
    border-bottom: 1px solid var(--hairline);
  }
  table.drill thead th:first-child { text-align: left; }
  table.drill td {
    padding: 9px 14px; text-align: right; white-space: nowrap;
    color: var(--text); font-variant-numeric: tabular-nums;
    border-bottom: 1px solid rgba(51,65,85,.45);
  }
  table.drill td:first-child { text-align: left; font-weight: 500; }
  table.drill td.muted { color: var(--muted); }
  table.drill tbody tr:hover td { background: rgba(212,175,55,.07); }
  table.drill tbody tr:last-child td { border-bottom: none; }

  .stButton > button, .stDownloadButton > button {
    background: linear-gradient(135deg, var(--gold) 0%, #b9942c 100%);
    color: #14110a; border: none; border-radius: 10px;
    font-weight: 700; font-size: .82rem; padding: .5rem 1.1rem;
    transition: filter .15s ease;
  }
  .stButton > button:hover, .stDownloadButton > button:hover { filter: brightness(1.08); color: #14110a; }

  /* ---------- Mobile ---------- */
  @media (max-width: 1450px) {
    .kpi-grid.cols-6, .kpi-grid.cols-5 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  }
  @media (max-width: 1100px) {
    .kpi-grid.cols-6, .kpi-grid.cols-5, .kpi-grid.cols-4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 820px) {
    .block-container { padding: .8rem .7rem 3rem; }
    .kpi-grid, .kpi-grid.cols-6, .kpi-grid.cols-5,
    .kpi-grid.cols-4, .kpi-grid.cols-3 { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 9px; }
    .kpi { padding: 13px 14px 12px; border-radius: 12px; }
    .kpi .value { font-size: 1.32rem; }
    .kpi .value.sm { font-size: 1.12rem; }
    .kpi .label { font-size: .62rem; letter-spacing: .1em; }
    .nani-head { padding: 12px 14px; gap: 12px; }
    .nani-head img { height: 40px; }
    .nani-head .title { font-size: 1.16rem; }
    .nani-head .stamp { text-align: left; width: 100%; }
    .stTabs [data-baseweb="tab"] { padding: 0 10px; font-size: .78rem; height: 38px; }
    div[data-testid="stDataFrame"] { overflow-x: auto; }
  }
  /* Two tiles per row is still readable on a 390px phone and halves the
     scroll depth; only genuinely narrow screens drop to a single column. */
  @media (max-width: 359px) {
    .kpi-grid, .kpi-grid.cols-6, .kpi-grid.cols-5,
    .kpi-grid.cols-4, .kpi-grid.cols-3, .kpi-grid.cols-2 { grid-template-columns: 1fr; }
    .kpi .value { font-size: 1.32rem; }
  }
</style>
"""


# ==============================================================================
#  SECTION 12 — UI COMPONENTS
# ==============================================================================

def kpi_tile(
    label: str,
    value: str,
    *,
    delta: Optional[Tuple[float, float]] = None,
    sub: str = "",
    accent: str = "gold",
    small: bool = False,
) -> str:
    """One KPI tile. `delta` is (current, previous); pass None to omit the badge."""
    badge_html = ""
    if delta is not None:
        cur, prev = delta
        abs_txt, pct_txt, direction = delta_parts(cur, prev)
        arrow = "▲" if direction == "up" else ("▼" if direction == "down" else "―")
        badge_html = (
            f'<div class="badge {direction}">{arrow} {pct_txt}'
            f'<span class="abs">({abs_txt})</span></div>'
        )
    accent_cls = "" if accent == "gold" else f" accent-{accent}"
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    val_cls = "value sm" if small else "value"
    return (
        f'<div class="kpi{accent_cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="{val_cls}">{value}</div>'
        f"{sub_html}{badge_html}"
        f"</div>"
    )


def render_kpi_grid(tiles: Sequence[str]) -> None:
    """Lay tiles out in one balanced row (or a clean 3+3 / 2+2+2 on narrower screens)."""
    cols = len(tiles) if 2 <= len(tiles) <= 6 else 0
    cls = f"kpi-grid cols-{cols}" if cols else "kpi-grid"
    st.markdown(f'<div class="{cls}">{"".join(tiles)}</div>', unsafe_allow_html=True)


def section_title(text: str) -> None:
    st.markdown(f'<div class="sec-title">{text}</div>', unsafe_allow_html=True)


def masthead(period_text: str, last_sync: str, channels_live: Sequence[str]) -> None:
    logo = logo_uri("Nani")
    img = f'<img src="{logo}" alt="Nani"/>' if logo else ""
    chips = "".join(f'<span class="chip">{c}</span>' for c in channels_live)
    st.markdown(
        f"""
        <div class="nani-head">
          {img}
          <div>
            <div class="title">{CFG.restaurant} — Executive Sales</div>
            <div class="subtitle">CFO Performance Console</div>
          </div>
          <div class="spacer"></div>
          <div class="stamp">
            <div><b>{period_text}</b></div>
            <div>Data synced {last_sync}</div>
            <div style="margin-top:6px">{chips}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def channel_header(channel: str) -> None:
    logo_key = {"Yandex": "Yandex", "Glovo": "Glovo", "Buy.am": "Buy.am"}.get(channel)
    uri = logo_uri(logo_key) if logo_key else ""
    img = f'<img src="{uri}" alt="{channel}"/>' if uri else (
        f'<span class="dot" style="background:{CHANNEL_COLORS.get(channel, Palette.GOLD)}"></span>'
    )
    caption = {
        "Hall": "Dine-in, takeaway and all in-house tenders",
        "Yandex": "Yandex Eats delivery aggregator",
        "Glovo": "Glovo delivery aggregator",
        "Buy.am": "Buy.am marketplace delivery",
    }.get(channel, "")
    st.markdown(
        f'<div class="ch-head">{img}<div><div class="name">{channel}</div>'
        f'<div class="note">{caption}</div></div></div>',
        unsafe_allow_html=True,
    )


# ------------------------------------------------------------------ charts ---

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color=Palette.TEXT_MUTED, size=12),
    margin=dict(l=10, r=10, t=30, b=10),
    hoverlabel=dict(bgcolor=Palette.SLATE, bordercolor=Palette.SLATE_3, font=dict(color=Palette.TEXT)),
)


def channel_mix_chart(mix: pd.DataFrame) -> Any:
    """Donut of net sales by channel, with the total in the hole."""
    total = float(mix["net_sales"].sum())
    fig = go.Figure(
        go.Pie(
            labels=mix["channel"],
            values=mix["net_sales"],
            hole=0.66,
            marker=dict(
                colors=[CHANNEL_COLORS.get(c, Palette.GOLD) for c in mix["channel"]],
                line=dict(color=Palette.OBSIDIAN, width=2),
            ),
            textinfo="percent",
            textfont=dict(size=12, color=Palette.OBSIDIAN, family="Inter"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f} ֏<br>%{percent}<extra></extra>",
            sort=False,
        )
    )
    fig.update_layout(
        **PLOT_LAYOUT,
        showlegend=True,
        legend=dict(orientation="h", y=-0.12, x=0.5, xanchor="center"),
        height=330,
        annotations=[
            dict(
                text=f"<b>{fmt_compact(total)}</b><br><span style='font-size:11px;color:{Palette.TEXT_FAINT}'>NET SALES</span>",
                x=0.5, y=0.5, font=dict(size=17, color=Palette.TEXT), showarrow=False,
            )
        ],
    )
    return fig


def trend_chart(df: pd.DataFrame, grain: str, by_channel: bool = False) -> Any:
    """Net sales over time — stacked by channel on the overview, plain elsewhere."""
    b = add_period_bucket(df, grain)
    fig = go.Figure()
    if by_channel:
        present = [c for c in REVENUE_CHANNELS if c in set(b["channel"])]
        for ch in present:
            sub = (
                b[b["channel"] == ch]
                .groupby("period", as_index=False)["net_sales"].sum()
                .sort_values("period")
            )
            fig.add_bar(
                x=sub["period"], y=sub["net_sales"], name=ch,
                marker_color=CHANNEL_COLORS.get(ch, Palette.GOLD),
                hovertemplate="<b>" + ch + "</b><br>%{x|%d %b %Y}<br>%{y:,.0f} ֏<extra></extra>",
            )
        fig.update_layout(barmode="stack")
    else:
        agg = b.groupby("period", as_index=False)["net_sales"].sum().sort_values("period")
        fig.add_bar(
            x=agg["period"], y=agg["net_sales"], name="Net sales",
            marker_color=Palette.GOLD,
            hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f} ֏<extra></extra>",
        )
        if len(agg) >= 7:
            roll = agg["net_sales"].rolling(7, min_periods=3).mean()
            fig.add_scatter(
                x=agg["period"], y=roll, name="7-period average",
                mode="lines", line=dict(color=Palette.TEXT_MUTED, width=2, dash="dot"),
                hovertemplate="avg %{y:,.0f} ֏<extra></extra>",
            )
    fig.update_layout(
        **PLOT_LAYOUT,
        height=300,
        showlegend=True,
        legend=dict(orientation="h", y=1.12, x=0, traceorder="normal"),
        xaxis=dict(showgrid=False, linecolor=Palette.SLATE_3, tickfont=dict(size=11)),
        yaxis=dict(gridcolor="rgba(51,65,85,.55)", zeroline=False, tickformat=",.0f", tickfont=dict(size=11)),
        bargap=0.25,
    )
    return fig


def payment_breakdown_chart(df: pd.DataFrame) -> Any:
    """Horizontal bars of the raw tender types inside one channel."""
    agg = (
        df.groupby("payment_type", as_index=False)["net_sales"].sum()
        .sort_values("net_sales", ascending=True).tail(12)
    )
    fig = go.Figure(
        go.Bar(
            x=agg["net_sales"], y=agg["payment_type"], orientation="h",
            marker_color=Palette.GOLD, opacity=0.85,
            hovertemplate="<b>%{y}</b><br>%{x:,.0f} ֏<extra></extra>",
        )
    )
    fig.update_layout(
        **PLOT_LAYOUT,
        height=max(200, 34 * len(agg) + 60),
        showlegend=False,
        xaxis=dict(gridcolor="rgba(51,65,85,.55)", zeroline=False, tickformat=",.0f"),
        yaxis=dict(showgrid=False, tickfont=dict(size=11)),
    )
    return fig


def style_drilldown(table: pd.DataFrame) -> pd.DataFrame:
    """Format the drill-down table's numeric columns for on-screen reading."""
    if table.empty:
        return table
    disp = table.copy()
    disp["Gross Sales"] = disp["Gross Sales"].map(lambda v: fmt_money(v))
    disp["Net Sales"] = disp["Net Sales"].map(lambda v: fmt_money(v))
    disp["Orders"] = disp["Orders"].map(fmt_int)
    disp["Average Check"] = disp["Average Check"].map(lambda v: fmt_money(v))
    disp["COGS"] = disp["COGS"].map(lambda v: fmt_money(v))
    disp["COGS %"] = disp["COGS %"].map(lambda v: fmt_pct(v))
    return disp


def _esc(s: Any) -> str:
    return (
        str(s).replace("&", "&amp;").replace("<", "&lt;")
        .replace(">", "&gt;").replace('"', "&quot;")
    )


def drilldown_html(table: pd.DataFrame) -> str:
    """Render the drill-down as a themed, scrollable HTML table."""
    disp = style_drilldown(table)
    cols = list(disp.columns)
    head = "".join(f"<th>{_esc(c)}</th>" for c in cols)
    muted_cols = {"COGS", "COGS %"}
    body = []
    for _, r in disp.iterrows():
        cells = "".join(
            f'<td class="muted">{_esc(r[c])}</td>' if c in muted_cols else f"<td>{_esc(r[c])}</td>"
            for c in cols
        )
        body.append(f"<tr>{cells}</tr>")
    return (
        '<div class="tbl-wrap"><table class="drill">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody>"
        "</table></div>"
    )


# ==============================================================================
#  SECTION 13 — VIEWS
# ==============================================================================

def render_kpis(cur: Metrics, prev: Optional[Metrics], compare: bool) -> None:
    """The KPI row.

    Sales and order metrics carry growth badges; COGS and COGS % deliberately
    do not. Cost ratios move for reasons (menu mix, stock-takes, supplier
    timing) that a period-over-period arrow misreads as performance.
    """
    def d(cur_v: float, prev_v: float) -> Optional[Tuple[float, float]]:
        return (cur_v, prev_v) if (compare and prev is not None) else None

    p = prev or Metrics()
    tiles = [
        kpi_tile("Net Sales", fmt_compact(cur.net_sales),
                 delta=d(cur.net_sales, p.net_sales),
                 sub="after discount", accent="gold"),
        kpi_tile("Gross Sales", fmt_compact(cur.gross_sales),
                 delta=d(cur.gross_sales, p.gross_sales),
                 sub="before discount", accent="gold"),
        kpi_tile("Order Count", fmt_int(cur.order_count),
                 delta=d(cur.order_count, p.order_count),
                 sub="checks", accent="gold"),
        kpi_tile("Average Check", fmt_money(cur.avg_check),
                 delta=d(cur.avg_check, p.avg_check),
                 sub="net ÷ orders", accent="gold", small=True),
        kpi_tile("COGS", fmt_compact(cur.cogs),
                 sub="cost of goods sold", accent="muted", small=True),
        kpi_tile("COGS %", fmt_pct(cur.cogs_pct),
                 sub="of net sales", accent="muted", small=True),
    ]
    render_kpi_grid(tiles)


def render_secondary_kpis(cur: Metrics, prev: Optional[Metrics], compare: bool, compliments: Metrics) -> None:
    def d(cur_v: float, prev_v: float) -> Optional[Tuple[float, float]]:
        return (cur_v, prev_v) if (compare and prev is not None) else None

    p = prev or Metrics()
    tiles = [
        kpi_tile("Discounts", fmt_compact(cur.discounts),
                 delta=d(cur.discounts, p.discounts),
                 sub="gross − net", accent="muted", small=True),
        kpi_tile("Discount %", fmt_pct(cur.discount_pct),
                 sub="of gross sales", accent="muted", small=True),
        kpi_tile("Gross Profit", fmt_compact(cur.gross_profit),
                 delta=d(cur.gross_profit, p.gross_profit),
                 sub="net − COGS", accent="emerald", small=True),
        kpi_tile("Gross Margin", fmt_pct(cur.gross_margin_pct),
                 sub="of net sales", accent="emerald", small=True),
        kpi_tile("Compliments Cost", fmt_compact(compliments.cogs),
                 sub=f"{fmt_int(compliments.order_count)} checks · excluded from revenue",
                 accent="crimson", small=True),
        kpi_tile("Compliments Value", fmt_compact(compliments.gross_sales),
                 sub="menu value given away", accent="crimson", small=True),
    ]
    render_kpi_grid(tiles)


def render_drilldown(df: pd.DataFrame, grain: str, title: str, period_text: str, key: str) -> None:
    section_title(f"{title} — {grain.lower()} detail")
    table = drilldown_table(df, grain)
    if table.empty:
        st.info("No transactions recorded for this selection.")
        return

    st.markdown(drilldown_html(table), unsafe_allow_html=True)
    unit = {"Daily": "days", "Weekly": "weeks", "Monthly": "months"}.get(grain, "periods")
    st.markdown(
        f'<div class="note">{len(table)} {unit} · scroll the table sideways on a narrow screen.</div>',
        unsafe_allow_html=True,
    )

    mix = (
        revenue_only(df).groupby("channel", as_index=False)["net_sales"].sum()
        if "channel" in df.columns else None
    )
    try:
        xlsx = build_excel(table, compute_metrics(df), title, period_text, mix)
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M")
        st.download_button(
            "⬇  Download Excel report",
            data=xlsx,
            file_name=f"Nani_{title.replace(' ', '_').replace('.', '')}_{grain}_{stamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"dl_{key}",
            use_container_width=False,
        )
    except Exception as exc:
        st.warning(f"Excel export unavailable: {exc}")


def view_overview(
    data: pd.DataFrame,
    lo: dt.date, hi: dt.date,
    plo: dt.date, phi: dt.date,
    compare: bool, grain: str, period_text: str,
) -> None:
    cur_all = slice_period(data, lo, hi)
    prev_all = slice_period(data, plo, phi)

    cur_rev = revenue_only(cur_all)
    prev_rev = revenue_only(prev_all)

    m_cur = compute_metrics(cur_rev)
    m_prev = compute_metrics(prev_rev)
    m_comp = compute_metrics(cur_all[cur_all["channel"] == "Compliments"])

    section_title("Consolidated performance — all channels")
    render_kpis(m_cur, m_prev, compare)
    render_secondary_kpis(m_cur, m_prev, compare, m_comp)

    if compare:
        st.markdown(
            f'<div class="note">Compared against <b>{plo:%d %b %Y} – {phi:%d %b %Y}</b>'
            f" ({(phi - plo).days + 1} days). Cost metrics are shown without growth"
            " badges by design.</div>",
            unsafe_allow_html=True,
        )

    if cur_rev.empty:
        st.info("No sales in the selected period.")
        return

    left, right = st.columns([1, 1.45], gap="medium")

    with left:
        section_title("Channel mix")
        mix = (
            cur_rev.groupby("channel", as_index=False)
            .agg(net_sales=("net_sales", "sum"), order_count=("order_count", "sum"))
        )
        mix["__order"] = mix["channel"].map({c: i for i, c in enumerate(REVENUE_CHANNELS)}).fillna(99)
        mix = mix.sort_values("__order").drop(columns="__order")
        if _HAS_PLOTLY:
            st.plotly_chart(channel_mix_chart(mix), use_container_width=True,
                            config={"displayModeBar": False})
        total_net = float(mix["net_sales"].sum()) or 1.0
        rows = []
        for _, r in mix.iterrows():
            ch = str(r["channel"])
            share = float(r["net_sales"]) / total_net
            dot = CHANNEL_COLORS.get(ch, Palette.GOLD)
            rows.append(
                f'<tr><td style="padding:5px 0"><span class="dot" style="display:inline-block;'
                f'width:8px;height:8px;border-radius:50%;background:{dot};margin-right:8px"></span>'
                f'{ch}</td><td style="text-align:right;color:{Palette.TEXT}">{fmt_money(r["net_sales"])}</td>'
                f'<td style="text-align:right;color:{Palette.TEXT_MUTED}">{fmt_pct(share)}</td></tr>'
            )
        st.markdown(
            '<div class="panel"><table style="width:100%;font-size:.82rem;border-collapse:collapse">'
            + "".join(rows) + "</table></div>",
            unsafe_allow_html=True,
        )

    with right:
        section_title(f"Net sales trend — {grain.lower()}")
        if _HAS_PLOTLY:
            st.plotly_chart(trend_chart(cur_rev, grain, by_channel=True),
                            use_container_width=True, config={"displayModeBar": False})

        section_title("Channel scorecard")
        score_rows = []
        for ch in REVENUE_CHANNELS:
            c_m = compute_metrics(cur_rev[cur_rev["channel"] == ch])
            p_m = compute_metrics(prev_rev[prev_rev["channel"] == ch])
            if c_m.net_sales == 0 and p_m.net_sales == 0:
                continue
            if compare:
                _, pct_txt, direction = delta_parts(c_m.net_sales, p_m.net_sales)
                colour = {"up": Palette.EMERALD, "down": Palette.CRIMSON}.get(direction, Palette.TEXT_MUTED)
                arrow = {"up": "▲", "down": "▼"}.get(direction, "―")
                delta_cell = f'<td style="text-align:right;color:{colour}">{arrow} {pct_txt}</td>'
            else:
                delta_cell = '<td style="text-align:right;color:#64748B">—</td>'
            score_rows.append(
                f'<tr style="border-top:1px solid {Palette.SLATE_3}">'
                f'<td style="padding:7px 0"><span style="display:inline-block;width:8px;height:8px;'
                f'border-radius:50%;background:{CHANNEL_COLORS.get(ch)};margin-right:8px"></span>{ch}</td>'
                f'<td style="text-align:right">{fmt_money(c_m.net_sales)}</td>'
                f'<td style="text-align:right">{fmt_int(c_m.order_count)}</td>'
                f'<td style="text-align:right">{fmt_money(c_m.avg_check)}</td>'
                f'<td style="text-align:right;color:{Palette.TEXT_MUTED}">{fmt_pct(c_m.cogs_pct)}</td>'
                f"{delta_cell}</tr>"
            )
        header = (
            f'<tr style="color:{Palette.TEXT_FAINT};font-size:.68rem;text-transform:uppercase;'
            'letter-spacing:.1em"><td>Channel</td><td style="text-align:right">Net sales</td>'
            '<td style="text-align:right">Orders</td><td style="text-align:right">Avg check</td>'
            '<td style="text-align:right">COGS %</td><td style="text-align:right">vs prev</td></tr>'
        )
        st.markdown(
            '<div class="panel"><table style="width:100%;font-size:.82rem;border-collapse:collapse">'
            + header + "".join(score_rows) + "</table></div>",
            unsafe_allow_html=True,
        )

    render_drilldown(cur_rev, grain, "All channels", period_text, key="overview")


def view_channel(
    data: pd.DataFrame, channel: str,
    lo: dt.date, hi: dt.date,
    plo: dt.date, phi: dt.date,
    compare: bool, grain: str, period_text: str,
) -> None:
    cur = slice_period(data, lo, hi, channel=channel)
    prev = slice_period(data, plo, phi, channel=channel)
    m_cur = compute_metrics(cur)
    m_prev = compute_metrics(prev)

    channel_header(channel)

    if cur.empty and prev.empty:
        st.info(f"No {channel} transactions in the selected period.")
        return

    render_kpis(m_cur, m_prev, compare)

    # Share of the whole business, for context.
    total_net = float(revenue_only(slice_period(data, lo, hi))["net_sales"].sum())
    share = (m_cur.net_sales / total_net) if total_net else 0.0
    tiles = [
        kpi_tile("Share of Net Sales", fmt_pct(share),
                 sub="of all revenue channels", accent="gold", small=True),
        kpi_tile("Discounts", fmt_compact(m_cur.discounts),
                 delta=(m_cur.discounts, m_prev.discounts) if compare else None,
                 sub=f"{fmt_pct(m_cur.discount_pct)} of gross", accent="muted", small=True),
        kpi_tile("Gross Profit", fmt_compact(m_cur.gross_profit),
                 delta=(m_cur.gross_profit, m_prev.gross_profit) if compare else None,
                 sub="net − COGS", accent="emerald", small=True),
        kpi_tile("Gross Margin", fmt_pct(m_cur.gross_margin_pct),
                 sub="of net sales", accent="emerald", small=True),
    ]
    render_kpi_grid(tiles)

    # A tender breakdown only says something when the channel actually takes
    # more than one tender — an aggregator settles as a single payment type, so
    # the chart would be one bar. Give the trend the full width instead.
    tenders = cur["payment_type"].nunique() if not cur.empty else 0
    if tenders > 1:
        left, right = st.columns([1.5, 1], gap="medium")
        with left:
            section_title(f"Net sales trend — {grain.lower()}")
            if _HAS_PLOTLY and not cur.empty:
                st.plotly_chart(trend_chart(cur, grain, by_channel=False),
                                use_container_width=True, config={"displayModeBar": False})
        with right:
            section_title("Tender breakdown")
            if _HAS_PLOTLY:
                st.plotly_chart(payment_breakdown_chart(cur), use_container_width=True,
                                config={"displayModeBar": False})
    else:
        section_title(f"Net sales trend — {grain.lower()}")
        if _HAS_PLOTLY and not cur.empty:
            st.plotly_chart(trend_chart(cur, grain, by_channel=False),
                            use_container_width=True, config={"displayModeBar": False})
        if tenders == 1:
            st.markdown(
                f'<div class="note">Settled as a single tender: '
                f"<b>{_esc(cur['payment_type'].iloc[0])}</b>.</div>",
                unsafe_allow_html=True,
            )

    render_drilldown(cur, grain, channel, period_text, key=f"ch_{channel}")


def view_compliments(
    data: pd.DataFrame, lo: dt.date, hi: dt.date,
    plo: dt.date, phi: dt.date, compare: bool, grain: str, period_text: str,
) -> None:
    cur = slice_period(data, lo, hi, channel="Compliments")
    prev = slice_period(data, plo, phi, channel="Compliments")
    m_cur, m_prev = compute_metrics(cur), compute_metrics(prev)

    st.markdown(
        '<div class="ch-head"><span class="dot" style="background:#64748B"></span>'
        '<div><div class="name">Compliments</div><div class="note">Zero-tender checks '
        "(без оплаты) — carried as a cost of hospitality, never as revenue.</div></div></div>",
        unsafe_allow_html=True,
    )

    if cur.empty and prev.empty:
        st.info("No complimentary checks in the selected period.")
        return

    total_net = float(revenue_only(slice_period(data, lo, hi))["net_sales"].sum())
    tiles = [
        kpi_tile("Cost of Compliments", fmt_compact(m_cur.cogs),
                 sub="COGS given away", accent="crimson"),
        kpi_tile("Menu Value", fmt_compact(m_cur.gross_sales),
                 delta=(m_cur.gross_sales, m_prev.gross_sales) if compare else None,
                 sub="at list price", accent="crimson"),
        kpi_tile("Checks", fmt_int(m_cur.order_count),
                 delta=(m_cur.order_count, m_prev.order_count) if compare else None,
                 sub="complimentary orders", accent="muted"),
        kpi_tile("Avg Value / Check", fmt_money(m_cur.gross_sales / m_cur.order_count if m_cur.order_count else 0),
                 sub="menu value", accent="muted", small=True),
        kpi_tile("Cost vs Net Sales", fmt_pct(m_cur.cogs / total_net if total_net else 0),
                 sub="compliment COGS ÷ revenue", accent="muted", small=True),
    ]
    render_kpi_grid(tiles)

    if _HAS_PLOTLY and not cur.empty:
        section_title(f"Compliment cost trend — {grain.lower()}")
        st.plotly_chart(trend_chart(cur, grain, by_channel=False),
                        use_container_width=True, config={"displayModeBar": False})

    render_drilldown(cur, grain, "Compliments", period_text, key="compliments")


# ==============================================================================
#  SECTION 14 — SIDEBAR
# ==============================================================================

@dataclass
class Selection:
    lo: dt.date
    hi: dt.date
    prev_lo: dt.date
    prev_hi: dt.date
    compare: bool
    grain: str
    preset: str
    snapped: bool


def render_sidebar(data: pd.DataFrame) -> Selection:
    today = dt.date.today()
    presets = build_period_presets(today)

    with st.sidebar:
        st.markdown(
            '<div style="font-size:.72rem;letter-spacing:.18em;text-transform:uppercase;'
            'color:#D4AF37;font-weight:700;margin-bottom:12px">Reporting controls</div>',
            unsafe_allow_html=True,
        )

        preset = st.selectbox("Period", list(presets.keys()), index=list(presets.keys()).index("This month"))
        lo, hi, cmp_mode = presets[preset]

        if preset == "Custom range":
            picked = st.date_input(
                "Custom date range",
                value=(lo, hi),
                min_value=dt.date(today.year - 3, 1, 1),
                max_value=today,
            )
            if isinstance(picked, (tuple, list)) and len(picked) == 2:
                lo, hi = picked[0], picked[1]
            elif isinstance(picked, dt.date):
                lo = hi = picked

        if lo > hi:
            lo, hi = hi, lo

        snap = False
        if preset == "Custom range":
            snap = st.toggle(
                "Snap to full calendar weeks",
                value=True,
                help=(
                    "Expands the range outwards to whole Monday–Sunday weeks so the "
                    "current and comparison windows contain the same number of each "
                    "weekday. Without this, a range holding six Saturdays is compared "
                    "against one holding five."
                ),
            )
            if snap:
                lo, hi = snap_to_full_weeks(lo, hi)
                hi = min(hi, today)

        grain = st.radio("Detail grain", GRAIN_LABELS, index=0, horizontal=True)

        st.markdown("---")
        compare = st.toggle("Compare with previous period", value=True)
        prev_lo, prev_hi = previous_period(lo, hi, cmp_mode if preset != "Custom range" else "auto")

        if compare:
            st.markdown(
                f'<div class="note">Current&nbsp;&nbsp;<b style="color:#F1F5F9">'
                f"{lo:%d %b} – {hi:%d %b %Y}</b><br>Previous&nbsp;&nbsp;"
                f'<b style="color:#94A3B8">{prev_lo:%d %b} – {prev_hi:%d %b %Y}</b></div>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown(
            '<div style="font-size:.72rem;letter-spacing:.18em;text-transform:uppercase;'
            'color:#D4AF37;font-weight:700;margin-bottom:8px">Data</div>',
            unsafe_allow_html=True,
        )
        c_lo, c_hi, n_rows = cache_span()
        last_sync = _meta_get("last_sync_at") or "never"
        err = _meta_get("last_sync_error")
        st.markdown(
            f'<div class="note">Cache&nbsp; <b>{n_rows:,}</b> rows<br>'
            f"Span&nbsp;&nbsp; {c_lo or '—'} → {c_hi or '—'}<br>"
            f"Last sync&nbsp; {last_sync}</div>",
            unsafe_allow_html=True,
        )
        if err:
            st.markdown(
                f'<div class="note" style="color:#EF4444;margin-top:6px">Last sync error: {err[:240]}</div>',
                unsafe_allow_html=True,
            )

        b1, b2 = st.columns(2)
        with b1:
            if st.button("Refresh", use_container_width=True, help="Pull the last few days from iiko"):
                with st.spinner("Syncing from iiko…"):
                    res = sync(full=False)
                st.session_state["_sync_result"] = res
                st.cache_data.clear()
                st.rerun()
        with b2:
            if st.button("Full rebuild", use_container_width=True, help="Re-pull the entire year to date"):
                with st.spinner("Rebuilding year-to-date cache…"):
                    res = sync(full=True)
                st.session_state["_sync_result"] = res
                st.cache_data.clear()
                st.rerun()

        with st.expander("Connection"):
            st.markdown(f'<div class="note"><b>app.py</b>: v{APP_VERSION}</div>',
                        unsafe_allow_html=True)
            for k, v in CFG.masked().items():
                st.markdown(f'<div class="note"><b>{k}</b>: {v}</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="note" style="margin-top:8px"><b>OLAP endpoint</b>: '
                f'{_meta_get("endpoint_olap") or "not yet discovered"}<br>'
                f'<b>first candidate</b>: '
                f'{_ordered_candidates(CFG.olap_endpoint, _meta_get("endpoint_olap"), OLAP_ENDPOINTS)[0]}'
                "</div>",
                unsafe_allow_html=True,
            )

    return Selection(lo, hi, prev_lo, prev_hi, compare, grain, preset, snap)


# ==============================================================================
#  SECTION 15 — APPLICATION ENTRY
# ==============================================================================

def _cached_sales() -> pd.DataFrame:
    """Sales mirror, memoised for five minutes so tab switches are instant."""
    return load_sales()


if _HAS_STREAMLIT:
    _cached_sales = st.cache_data(ttl=300, show_spinner=False)(_cached_sales)


STREAMLIT_CONFIG = """# Written automatically by app.py on first run.
[theme]
base                = "dark"
primaryColor        = "#D4AF37"
backgroundColor     = "#0F172A"
secondaryBackgroundColor = "#1E293B"
textColor           = "#F1F5F9"

[server]
headless            = true

[browser]
gatherUsageStats    = false
"""


def ensure_streamlit_config() -> bool:
    """Write .streamlit/config.toml if absent. Returns True when newly created.

    Streamlit reads its theme at server start, so a freshly written config only
    takes effect on the next launch. The CSS above covers the first run.
    """
    path = APP_DIR / ".streamlit" / "config.toml"
    if path.exists():
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(STREAMLIT_CONFIG, encoding="utf-8")
        log.info("wrote %s — restart Streamlit to pick up the dark theme", path)
        return True
    except Exception as exc:
        log.warning("could not write streamlit config: %s", exc)
        return False


def run_app() -> None:
    fresh_config = ensure_streamlit_config()
    st.set_page_config(
        page_title=f"{CFG.restaurant} — Executive Sales",
        page_icon="◆",
        layout="wide",
        # "auto" keeps the control rail open on a desktop and collapsed on a
        # phone, where it would otherwise cover the whole report.
        initial_sidebar_state="auto",
    )
    st.markdown(CSS, unsafe_allow_html=True)

    init_db()

    # First run: bootstrap the year-to-date cache. Afterwards: one quiet
    # incremental pull per calendar day, on the first load of the morning.
    if "_boot_done" not in st.session_state:
        st.session_state["_boot_done"] = True
        lo, _, _ = cache_span()
        if lo is None:
            with st.spinner("First run — loading year-to-date sales from iiko…"):
                st.session_state["_sync_result"] = sync(full=True)
        else:
            with st.spinner("Checking for new figures…"):
                res = sync_if_stale()
                if res:
                    st.session_state["_sync_result"] = res
        st.cache_data.clear()

    res = st.session_state.pop("_sync_result", None)
    data = _cached_sales()

    sel = render_sidebar(data)

    period_text = f"{sel.lo:%d %b %Y} – {sel.hi:%d %b %Y}"
    live = sorted({c for c in data["channel"].unique() if c in REVENUE_CHANNELS}) if not data.empty else []
    masthead(period_text, _meta_get("last_sync_at") or "never", live or ["No data"])

    if fresh_config:
        st.info(
            "Theme settings written to `.streamlit/config.toml`. Restart "
            "`streamlit run app.py` once to apply them fully."
        )

    if res:
        if res.get("ok"):
            st.success(f"Sync complete — {res['mode']}, {res['rows']:,} rows in {res.get('seconds', 0)}s.")
        else:
            st.error(f"Sync failed ({res['mode']}): {res.get('error', 'unknown error')}")

    if data.empty:
        st.warning(
            "The local cache is empty. Use **Full rebuild** in the sidebar to pull the "
            "year to date from iiko, or run `python app.py doctor` to diagnose the "
            "API connection."
        )
        with st.expander("Diagnostics"):
            st.code("\n".join(IikoClient().diagnostics) or "No diagnostics recorded yet.")
        return

    # Buy.am gets a tab only when it actually trades; an empty tab reads as a
    # broken integration rather than as a channel the venue has not launched.
    window = slice_period(data, sel.lo, sel.hi)
    full_window = slice_period(data, min(sel.lo, sel.prev_lo), max(sel.hi, sel.prev_hi))
    active_delivery = [
        ch for ch in DELIVERY_CHANNELS
        if float(full_window.loc[full_window["channel"] == ch, "net_sales"].sum()) > 0
        or int(full_window.loc[full_window["channel"] == ch, "order_count"].sum()) > 0
    ]
    has_compliments = float(window.loc[window["channel"] == "Compliments", "gross_sales"].sum()) > 0

    tab_names = ["Executive Overview", "Hall"] + active_delivery + (["Compliments"] if has_compliments else [])
    tabs = st.tabs(tab_names)

    with tabs[0]:
        view_overview(data, sel.lo, sel.hi, sel.prev_lo, sel.prev_hi, sel.compare, sel.grain, period_text)
    with tabs[1]:
        view_channel(data, "Hall", sel.lo, sel.hi, sel.prev_lo, sel.prev_hi, sel.compare, sel.grain, period_text)
    for i, ch in enumerate(active_delivery, start=2):
        with tabs[i]:
            view_channel(data, ch, sel.lo, sel.hi, sel.prev_lo, sel.prev_hi, sel.compare, sel.grain, period_text)
    if has_compliments:
        with tabs[-1]:
            view_compliments(data, sel.lo, sel.hi, sel.prev_lo, sel.prev_hi, sel.compare, sel.grain, period_text)

    st.markdown(
        f'<div class="note" style="margin-top:34px;text-align:center">'
        f"{CFG.restaurant} · {CFG.preset_name} OLAP feed · {CFG.base_url} · "
        f"generated {dt.datetime.now():%d %b %Y %H:%M}</div>",
        unsafe_allow_html=True,
    )


# ==============================================================================
#  SECTION 16 — HEADLESS CLI
# ==============================================================================

def cmd_doctor() -> int:
    """Probe the configured host and report exactly what is reachable."""
    print(f"\n=== Nani dashboard — connection doctor (app.py v{APP_VERSION}) ===\n")
    for k, v in CFG.masked().items():
        print(f"  {k:<12} {v}")
    pid_probe = CFG.preset_id or _meta_get("preset_id") or ""
    print(f"\n  PRIMARY OLAP route: POST {PRIMARY_OLAP_ENDPOINT}")
    print(f"  preset id in use  : {pid_probe or '(none — preset routes will be skipped)'}")
    if pid_probe:
        print(f"  resolves to       : POST {PRIMARY_OLAP_ENDPOINT.format(preset_id=pid_probe)}")
    print("\n  Report paths will be tried in this order:")
    for i, path in enumerate(
        _ordered_candidates(CFG.olap_endpoint, _meta_get("endpoint_olap"), OLAP_ENDPOINTS), 1
    ):
        mark = "   <- primary" if path == PRIMARY_OLAP_ENDPOINT else ""
        if "{preset_id}" in path and not pid_probe:
            mark = "   (skipped: no preset id)"
        print(f"    {i}. {path}{mark}")
    if BLOCKED_ENDPOINT_PREFIXES:
        print("  Never contacted (blocked):")
        for prefix in BLOCKED_ENDPOINT_PREFIXES:
            print(f"    · {prefix}  and everything beneath it")
    print()

    client = IikoClient()
    try:
        client.authenticate()
        print(f"  [ok]   authenticated via '{client.dialect['name']}' dialect")
        print(f"         login path: {client.dialect['auth']}")
    except Exception as exc:
        print(f"  [FAIL] authentication: {exc}")
        for line in client.diagnostics:
            print(f"         · {line}")
        return 2

    print("\n  --- endpoint discovery ---")
    try:
        presets = client.list_presets()
        print(f"  [ok]   presets endpoint: {client.presets_endpoint}")
        print(f"  [ok]   {len(presets)} OLAP presets visible")
        for p in presets[:25]:
            print(f"         · {p.get('name') or p.get('title') or '?'}  ({p.get('id') or '—'})")
    except Exception as exc:
        print(f"  [warn] no preset endpoint responded:\n         {exc}")

    pid = client.resolve_preset_id()
    print(f"  {'[ok]  ' if pid else '[warn]'} preset '{CFG.preset_name}' -> "
          f"{pid or 'not found (will use an explicit OLAP query)'}")

    today = dt.date.today()
    try:
        df = client.fetch_sales(today - dt.timedelta(days=7), today)
        used = client.preset_run_endpoint or client.olap_endpoint
        print(f"  [ok]   report endpoint: {used}")
        print(f"  [ok]   sample pull: {len(df)} rows over the last 7 days")
        if not df.empty:
            print(f"         channels: {sorted(df['channel'].unique())}")
            print(f"         tenders : {sorted(df['payment_type'].unique())[:12]}")
            print(f"         net sales: {df['net_sales'].sum():,.0f} {CURRENCY}")
        else:
            print("         (endpoint answered but returned no rows for this window)")
    except Exception as exc:
        print(f"  [FAIL] sample pull:\n         {exc}")
        return 3
    finally:
        client.logout()

    print("\n  All good. Run:  streamlit run app.py\n")
    return 0


def cmd_reset_endpoints() -> int:
    """Forget the remembered endpoints so the next run re-discovers them."""
    init_db()
    for key in ("endpoint_olap", "endpoint_presets", "endpoint_preset_run",
                "api_dialect", "preset_id"):
        _meta_set(key, "")
    print("Cached endpoint discovery cleared. The next sync will probe again.")
    return 0


def cmd_sync(full: bool) -> int:
    res = sync(full=full)
    if res.get("ok"):
        print(f"sync ok — {res['mode']}: {res['rows']:,} rows ({res['from']} .. {res['to']})")
        return 0
    print(f"sync FAILED — {res.get('error')}")
    return 1


def main(argv: Sequence[str]) -> int:
    args = list(argv[1:])
    if not args:
        print(__doc__)
        print("No sub-command given. Launch the dashboard with:  streamlit run app.py")
        return 0
    cmd = args[0].lower()
    if cmd == "sync":
        return cmd_sync(full="--full" in args)
    if cmd == "doctor":
        return cmd_doctor()
    if cmd in ("reset-endpoints", "reset"):
        return cmd_reset_endpoints()
    print(f"Unknown command '{cmd}'. Expected one of: sync, doctor, reset-endpoints.")
    return 64


# When Streamlit imports this module it sets __name__ to "__main__" as well, so
# distinguish a real CLI invocation by the presence of the streamlit runtime.
def _running_under_streamlit() -> bool:
    if not _HAS_STREAMLIT:
        return False
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


if __name__ == "__main__":
    if _running_under_streamlit():
        run_app()
    else:
        sys.exit(main(sys.argv))