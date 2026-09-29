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

 The request BODY is reproduced by _grouped_table_body(). Only dateFrom and
 dateTo inside the OpenDate.Typed filter change per request; everything else is
 byte-identical to the portal's own request, key order included.

 Three filters go out every time: the date window, plus OrderDeleted and
 DeletedWithWriteoff both pinned to NOT_DELETED so voided checks and write-offs
 never reach the revenue figures. Each filter carries the server's full key set
 — valueMin, valueMax, valueList, includeLeft, includeRight, inclusiveList —
 even where the values are null or empty. Those keys are required: a trimmed
 payload earns HTTP 400. Note includeRight differs by filter type (true on the
 date range, false on the value lists), so the two builders are separate.

 storeIds comes from IIKO_STORE_IDS (default 45159). The other shapes in
 _preset_bodies() are legacy fallbacks; the accepted one is cached in `meta`.

 dataFields asks for both ProductCostBase.ProductCost (an amount) and
 ProductCostBase.Percent (a ratio). The amount is used directly. The ratio is a
 fallback: if a row arrives without an amount, COGS is reconstructed as
 net x percent — or gross x percent when net is zero, since a complimentary
 check has no net sales and would otherwise be costed at zero.

 DishDiscountSumInt.average is the server's own average check. It is mapped so
 it is not logged as unknown, but the dashboard recomputes average check as
 net / orders so the figure always agrees with the two shown beside it.

 ONE THING WORTH KNOWING: includeNonBusinessPaymentTypes is false, matching the
 portal. That flag may exclude complimentary "(без оплаты)" checks entirely, in
 which case the Compliments tab stays empty. Set IIKO_INCLUDE_NON_BUSINESS=1 to
 include them — at the cost of no longer matching the portal exactly.

 ------------------------------------------------------------------------------
 BROWSER HEADERS
 ------------------------------------------------------------------------------
 The backend checks that a request looks like it came from its own SPA, and
 rejects a plain requests.Session. Every call therefore carries the portal's
 header set — Accept, Accept-Language, Origin, Referer, User-Agent — plus a
 fresh X-Correlation-Id (uuid4) per request, because the portal issues a new one
 per XHR and reusing one across a backfill would not resemble a browser.

 Origin and Referer are derived from IIKO_BASE_URL rather than hardcoded, so
 they cannot go stale if the host changes. IIKO_USER_AGENT, IIKO_REFERER_PATH
 and IIKO_ACCEPT_LANGUAGE override the defaults when the portal moves on.

 Payloads go out via requests' json= argument, so serialisation and encoding
 are handled by the library rather than by hand.

 The RESPONSE is a grouped table rather than a flat list, so _extract_rows
 handles both a columns + array-of-arrays table and a nested group tree, pushing
 each group's dimension values down onto its leaf rows. Its headers are field
 ids (OpenDate.Typed, PayTypes.Combo, …), which FIELD_ALIASES maps alongside
 the Russian ones.

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
import json
import logging
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
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


# The preset id lives in the OLAP route's PATH, so it is required, not optional.
# IIKO_PRESET_ID in the environment or .env is the source of truth; this is only
# the fallback when nothing is configured.
#
# Three different ids have been captured from this portal at different times:
#   38d44fc2e1bf180d53bada5bf7dd92bb2ca85713  <- from the working cURL (default)
#   1e881bc64c15eb038684d29286c6495f4bc2822e
#   7ac4dfd850327a8d89ab49c7000ba4d93a5263ce  <- first capture, called "Sales intr"
# They address different saved reports. Confirm which one is "Sales intr" and
# set IIKO_PRESET_ID accordingly rather than relying on this default.
DEFAULT_PRESET_ID = "38d44fc2e1bf180d53bada5bf7dd92bb2ca85713"

# Nani began trading on 1 May 2024. A full rebuild starts here, not at the top
# of the current year, so the cache holds the whole history and year-on-year
# comparison has something to compare against.
DEFAULT_HISTORY_START = "2024-05-01"


# --- Browser identity ---------------------------------------------------------
# The iikoWeb backend checks that a request looks like it came from its own SPA.
# A bare requests.Session is rejected, so every call reproduces the headers the
# portal itself sends. Captured verbatim from a working cURL; note that
# accept-language really does carry literal double quotes around ru_RU.
BROWSER_ACCEPT = "application/json, text/plain, */*"
BROWSER_ACCEPT_LANGUAGE = '"ru_RU"'
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
)
# The SPA page the XHR originates from. Origin and Referer are derived from
# IIKO_BASE_URL rather than hardcoded, so they stay consistent if the host moves.
BROWSER_REFERER_PATH = "/dashboard/index.html"


def _id_list(raw: Any) -> List[Any]:
    """Parse a store-id setting into the list the API expects.

    Numeric ids are sent as integers, because that is what the portal sends and
    a quoted "45159" is not guaranteed to be accepted. Non-numeric ids (a UUID,
    for instance, which other iiko deployments use) are kept as strings rather
    than discarded. An unset or unparseable value yields an empty list, which
    the server reads as "every store this account can see".
    """
    if isinstance(raw, (list, tuple)):
        chunks = [str(x) for x in raw]
    else:
        chunks = [c for c in re.split(r"[,\s\[\]\"\']+", str(raw or "")) if c]
    out: List[Any] = []
    for chunk in chunks:
        if not chunk:
            continue
        try:
            out.append(int(chunk))
        except ValueError:
            out.append(chunk)          # a UUID or other opaque identifier
    return out


# Backwards-compatible alias; _id_list supersedes it.
_int_list = _id_list


def _as_bool(raw: Any, default: bool = False) -> bool:
    """Read a flag from configuration into a real bool, never a string.

    "false" is a non-empty string and therefore truthy in Python — sending it
    straight into the JSON body would silently invert the setting.
    """
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off", ""):
        return False
    return default


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

    # --- OLAP request payload ------------------------------------------------
    # Values captured from the portal's own grouped-table request.
    store_ids: List[Any] = field(default_factory=lambda: _id_list(_cfg("IIKO_STORE_IDS", "45159")))
    # The portal sends both of these as false. Setting IIKO_INCLUDE_NON_BUSINESS=1
    # is what makes complimentary "(без оплаты)" checks appear in the feed —
    # without it the Compliments tab stays empty. See the note in the docstring.
    include_non_business: bool = field(
        default_factory=lambda: _as_bool(_cfg("IIKO_INCLUDE_NON_BUSINESS", "0")))
    include_void: bool = field(
        default_factory=lambda: _as_bool(_cfg("IIKO_INCLUDE_VOID", "0")))
    # Where a full rebuild begins. Defaults to the date Nani opened, so a full
    # rebuild pulls the entire trading history rather than the current year.
    # Every window before this answers 400 "Data not found", which costs a
    # request and clutters the log, so the floor is worth setting accurately.
    history_start: str = field(
        default_factory=lambda: _cfg("IIKO_HISTORY_START", DEFAULT_HISTORY_START))

    # --- browser identity ----------------------------------------------------
    # Overridable so a future Chrome version or portal path can be matched
    # without editing the source.
    user_agent: str = field(default_factory=lambda: _cfg("IIKO_USER_AGENT", BROWSER_USER_AGENT))
    accept_language: str = field(
        default_factory=lambda: _cfg("IIKO_ACCEPT_LANGUAGE", BROWSER_ACCEPT_LANGUAGE))
    referer_path: str = field(
        default_factory=lambda: _cfg("IIKO_REFERER_PATH", BROWSER_REFERER_PATH))

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

APP_VERSION = "2.3.0"

CURRENCY = "AMD"
CURRENCY_SYMBOL = "֏"


# ==============================================================================
#  SECTION 2 — DESIGN SYSTEM
# ==============================================================================

class Palette:
    """Executive light palette — warm off-white paper, ink-dark figures.

    Accent and status colours are darkened from their dark-theme values because
    a hue that reads brightly on obsidian is nearly invisible on white. Every
    colour used for text or a thin mark here clears 4.5:1 against the surface.
    """

    PAPER = "#F6F7F9"         # page background — whiteish, faintly cool
    PAPER_WARM = "#FBFBF9"    # secondary wash
    SURFACE = "#FFFFFF"       # card surface
    SURFACE_2 = "#F1F3F7"     # elevated / hover
    LINE = "#E3E7EE"          # hairlines
    LINE_SOFT = "#EDF0F5"

    INK = "#0F172A"           # primary text (obsidian, now as ink)
    TEXT = "#0F172A"
    TEXT_MUTED = "#5A6678"
    TEXT_FAINT = "#8A94A6"

    GOLD = "#A8801A"          # accent, darkened for contrast on white
    GOLD_SOFT = "#C9A227"
    GOLD_WASH = "#FBF3DC"
    EMERALD = "#047857"       # gains
    EMERALD_WASH = "#E6F5EF"
    CRIMSON = "#BE1E2D"       # losses
    CRIMSON_WASH = "#FCEBEC"

    # Retained so any dark-theme reference still resolves.
    OBSIDIAN = "#0F172A"
    SLATE = "#1E293B"
    SLATE_2 = "#E3E7EE"
    SLATE_3 = "#E3E7EE"

    # Channel identity. Hall becomes slate rather than gold: gold-on-white is
    # weak, and the house reads naturally as the neutral anchor.
    CH_HALL = "#334155"       # slate — the house
    CH_YANDEX = "#E0A800"     # Yandex amber
    CH_GLOVO = "#00907A"      # Glovo teal
    CH_BUYAM = "#D1105E"      # buy.am magenta
    CH_COMPLIMENTS = "#94A3B8"


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
    "Glovo": "iVBORw0KGgoAAAANSUhEUgAAAFgAAABYCAYAAABxlTA0AAAKx0lEQVR42u3ce3DU5b3H8ffz/C67m2wSyD0hJIEQkJsFESuDBkodsbaAVMVObZlSp5c/bMe2Zzwz7ek51k7nnDnn1DrVcTzTOa2Xjtqb1nYcilJtFREQjYDIJVwCuRBJDLnu5nd5nuf8sUkqQntmSJA/fD5//va3v919Pc9+n8tvZwWP/NBgc9EiLYEFtsA2FtgCW2AbC2yBLbCNBbbANhbYAltgGwtsgS2wjQW2wBbYxgJbYBsLbIEtsI0FtsAW2GaS41qCsZ5mkCL3O0htBBphgScrjjAo7aCVlzsgFFJGGATGAk+856o4wfREH8vyT5GUMXuzZbyVqQAZwwSJ3Y86rtY+3y5v5l+rtlPkD4DQEOfx2Hvz+Xr7SgIEBnPBzO5HuizECW4rPsiPa58H7fFyXyN9KskNBa1srHid0Ei+0nYdjhOgjLjARrxIEYBA/N2hQgCukEghLgmwNgIpY+4ua8ZguLfrala03Mq6o2tZ23ojmbCATcXv0JB8D6Vd5AX2YTn5PSOHZozBaIUxBiEErpBnYRtjiIMMWsWj3B9eBAZjJCVehtmJM8RRAQ/0LETICM8bZkt/A68PT0N6GRYkzoB2kIJLDyyFQIVZdBiQclxKknmkPR8TR8QjmXFsjCHpenxp7lJmFBaDjv9BX79Y3y/DiHbIaA8pQ2q8YYyRaCS+E1DpZhBGMqA9EFzaGpzD0egwZE39PDY1LmJp6TSm+EmG45BD/T08e+IgDx96k4xWEIV8rmEhv7hmHb85vp8NW59EJjyUMWcRMOEx/FzWsWs6UjMY5bN1aDqfL3+DH1dt5/pja1FRiu9Vb2dOfgdt2Qp2Z8oRMkJfYA12J6fWGoTW/E/TTdwx+4rxxwbCESpSaSpSaZoq69k4axHXPf9LeoIsxX6S2GjyXA+EGKeUIle5lVajL5ArL8rocezctyBHZc4BFLkeN9pYYrRsxUbnjglwhINAI6TiX7o+zg0FJ1g19RB3lu3lr0PT+H7lDjAu3zm1jME4heNe+CDnTrwsSFSQ4afL13LH7CtQWvMf+7bxaMtbnAkDirwEn66ZxfcWNdFYVEzSccEYlDGjcOasa+koAK3xEkl86TAchcTRCPgJhJQYrTFjdVvK0cb5W11Hq1xjSWf8/DjMgOuT7/kEWhEHGXBcPA+OjxRzZ0cTT8z4Iz8o38VAaQLXGeHnp5fwm965OG4WZS68kroTG9AEKgq4qmoG35h3FdoYvrlrCw81/wWSeSAkPWGWn769nWfaW/CkpCM7CI4z3vfE+3CjIMPishrumn81KyvrKPSTHB3o5XetB7j/nZ1kVYQvHR5beQvlyXw+s/VJslohhMQYTUo6PLf6C5wcHuCObc8Sq5i06/Oty5fz2bq51BdMpS/I8tKpVn6yfwf73juFn4Ane+fxqYI2vli+myIjOZKp4q7Oa5FOiDYTG6YmBCyEABXzxYaFAOzs7uCh/a/h5hehAR2HoBQISVtvV+5rnZcG84HKKiRROMK6+nk89YlbSTouWRVxZiTLktJqlpRWs7Z2Djc+/ziRNlxXPZOSZB5r6ubwq8PNJFJpRrJZ1s5ezMqqGZwc6sMxMDWRYvP1X2BJaTUAHcP91OQXsWn2YjbMnM+tL/6azScP4foOd3ZeQ6ETUOYN8a2OFQyqBNIJMUZcOmBlDLgei4orAPhTxxGEUkhfEMchc6dWsLSkEo3Bkw7aGLZ0HqcrOzQ+a8g1UkTNlHIebVpP0nH5z32v8t97tzEQxywtqeRn167j6vIa7rv6Rjb96XF+dvhN7l64nC81fIxfHdlDbDRIhztmLcIADxx4nSAzwIOfuIUlpdXsOt3OV1/9A4cGeinzk/zgyuvY1LiIx5rWM+/ph+gJMwyYFPe+u5Q6b5hdw1VIGV7wwDZp07TcoCFJuz4A/UH2fTN5zVMr1vNo03oeb/osP79mHY9cexM/WrwCohBntHY6QkAU8vmZCyjyk/z1VCv/vGMz3WFAJGBbewtf2/4cymhuq59HcXEFDx7cTagVn6yewWXFVcTZYeaVVLGyagaDYcD/Hm5mWlkNN9dexoiK2fTqH9lzup1QSNqyQ3x527Ps7e2iNJnPzfVzMWFMqT/Ca7N+x9NznmBVwUm09nGEubTAUgjQit4giwFq0kV/G9WF4J92b+Xf97zCvW+8yOb2FpTRTPWTo/OOsUbKnXv51HIM8ELnMSTgex4Yg5NXwI7uDjqGB0i5HpcXV9DR08nvTxzEkw63z5wPQZaNMxfgSskTx/Zxpu80i4srcKTk6EAv7/T3IFP5GKPx/QQiCtnScQyDYcHUckAzpHx+29/Ajr5GWoIihFDnVLIPH5hcDd7Z04kA1k2fg5dIEaoIz0vwQvsRvvvGVv7tted4uvUAjpAESp1vzk+kNRhD2vPRRueW2UIgjEFKgS8dAAIVI6TDgwd3A3DbjPkUTinlczMXoI3h4cNvIhyPaHSal+d6OEIg3zcFNEDSzVXHcPT9hMDtJ69nWcuttEVpjFCTsic8IWBNrgY/cmQPkVY0FpXwwMdvgDgiGhke657gOFSm0iij8UahPjj5397djhCCW+rnUphXSJAZRKmYeLif9dPnUJFK05UZZN+Z04hUPtu7WtnZ3U5jYQn/ddVq6tJT2Np5lD3d7ZDMo7n3NINRQG16CutqZxMP9aNUTJAZZGp+IWtqGhEIXj7dBlIiMQgRgVAIoSdt+e5w06p7LrgGA47j0jN4hmGtWT2tgStLq1lV3YArHcpS+Swtreauy5v46mVXknRcXuw6zubjb7N82ixWVc/kcP97PNm6n6NDfdxcP5fGwhKurajlVHaIfNdjQ+Ni7rvqepKux/eb/8LL7Ufw/SRxOMKw1txcP5dFJZVIIfjmri209HXj+wkGhgdI+ylWVNaxeloDw0YTxBFXlE3j4eVrWFhcwa7uDr67+wWM46ExmIuwXJ/wQkMZjZNIcd/eV4i14p7FK2mqrKOpsu6cAfEXh5v54Vsvg5cg0ApXytwgJwT9UcD6rU/x21UbuKails03bDzr+ffvf437396O9JOEKkb4CX5/4gBtQ/1MTxex/8xpnm9rQXoJIqWQfpJ73nyJmrwCNjYu4oFlnz7reru7O9jw0q8JtUG6YnzlN/m7HpP0hxxSCHSQpbaolJvqLuPKkiqm+EkGooDm3nfZ0n6Et3s6wcvdlilPprl7wTKeOXmIV7tacT2fOAwoTKS4vWEhTRW1FHgJDg/08syJA7zSeQzhJcYHRykEOgr5ZM0svtK4mJ+8s5Od755Eej56rDQZDSpi9fTZrJk+h/p0Eb3BCH8+dYynju0nUBHC9S4a7qQCj21VKhVBFOaWsKM7Z4zOlx03N4DlthE0hAG4HjgeYHJoWkMUjM9E0AYcifQSo3BnL3RMHIGKwXFz1zrfOeFI7vjY+wHwk7nHzMX9w5dJvaOhjEY6LtLxRntariflnAzK6Pd9cImTykcbMw6nR7cznUTe+PPF6C0bdR4IYwzS9RCuj8Gc0wBj5zh+cvw6Y3t/amzz5yJn0m8ZaWPQZ+1xmfPuORog1vr8x43+wJF//Hr/3znqrHM+3L8osj88scAW2MYCW2ALbGOBLbAFtrHAFtjGAltgC2xjgS2wBbaxwBbYAlsCC2yBbSywBbbANhbYAltgGwtsgW0ssAW2wDYW2AJ/JPJ/UHON6VxT8NcAAAAASUVORK5CYII=",
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
    # The portal's captured payload requests ProductCostBase.Percent, i.e. COGS
    # as a ratio rather than an amount, so absolute COGS is reconstructed from
    # it in normalise_olap_frame().
    "себест%": "cogs_pct_src",
    "себестоимость%": "cogs_pct_src",
    "productcostbasepercent": "cogs_pct_src",
    "productcostpercent": "cogs_pct_src",
    "срчек": "avg_check_src",
    "среднийчек": "avg_check_src",
    # The server's own average check. Mapped so it is recognised rather than
    # logged as an unknown column; the dashboard still recomputes it as
    # net ÷ orders so the figure always agrees with the two it is shown beside.
    # Distinct from "dishdiscountsumint" (net sales) after normalisation.
    "dishdiscountsumintaverage": "avg_check_src",
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
    # No preset in the path: used when the saved report is gone, which the
    # server reports as 400 "Request data not found" rather than a 404.
    "/api/olap/fetch/grouped-table",
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

# iikoWeb answers 400 — not 404 — when the preset named in the path no longer
# exists. The route is fine; the saved report behind it is gone. Treated as a
# reason to drop the preset from the path rather than to keep reshaping the body.
# "Request data not found" names the saved *request* — the preset — not the
# sales data. It is a different failure from "Data not found", which means the
# window is empty, and the two must not be conflated: one needs a new preset id,
# the other needs nothing at all.
_PRESET_MISSING_MARKERS = (
    "request data not found",
    "preset not found",
    "report not found",
    "preset does not exist",
)

# iikoWeb answers 400 "Data not found" when the requested window holds no
# trading at all. That is an ordinary empty period — a restaurant that opened in
# June has nothing in January — and NOT an error. It arrives as a 400 rather
# than a 200 with an empty row list, so a year-to-date backfill starting on
# 1 January would abort on its very first chunk and report the whole sync as
# failed, which is exactly what happened.
_NO_DATA_MARKERS = (
    "data not found",
    "no data",
    "нет данных",
)


def _is_preset_missing(status: int, text: str) -> bool:
    """True when the server says the saved report itself is gone."""
    if status not in (400, 404, 422):
        return False
    low = (text or "").lower()
    return any(marker in low for marker in _PRESET_MISSING_MARKERS)


def _is_no_data(status: int, text: str) -> bool:
    """True when the server says the window is empty rather than malformed.

    Deliberately checked AFTER _is_preset_missing, because "request data not
    found" contains "data not found" as a substring; a missing preset must not
    be mistaken for an empty week.
    """
    if status != 400:
        return False
    low = (text or "").lower()
    if "request data not found" in low:
        return False
    return any(marker in low for marker in _NO_DATA_MARKERS)


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


def _browser_headers(cfg: Config = CFG) -> Dict[str, str]:
    """The header set the iikoWeb backend expects from its own front end.

    Origin and Referer are built from the configured base URL so that pointing
    IIKO_BASE_URL at a different venue does not leave a stale host behind in
    headers the server cross-checks against the request.
    """
    origin = cfg.base_url.rstrip("/")
    return {
        "Accept": BROWSER_ACCEPT,
        "Accept-Language": cfg.accept_language,
        "Origin": origin,
        "Referer": _join_url(origin, cfg.referer_path),
        "User-Agent": cfg.user_agent,
    }


class IikoClient:
    """Resilient client for the iiko OLAP Sales report."""

    def __init__(self, cfg: Config = CFG):
        self.cfg = cfg
        self.session = requests.Session()
        # Present as the portal's own SPA on every call, not just the OLAP one —
        # the backend applies the same check to login and preset listing.
        self.session.headers.update(_browser_headers(self.cfg))
        self.token: Optional[str] = None
        self.dialect: Optional[Dict[str, Any]] = None
        # Report paths are discovered at run time and may belong to a different
        # dialect than the one that authenticated.
        self.olap_endpoint: Optional[str] = None
        self.presets_endpoint: Optional[str] = None
        self.preset_run_endpoint: Optional[str] = None
        self.body_shape: Optional[str] = None
        # Set when the server says the preset named in the path is missing.
        self.preset_missing: bool = False
        # Set when a window came back explicitly empty rather than failing.
        self.empty_window: bool = False
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

    def _auth_params(self, path: str = "") -> Dict[str, str]:
        """Query params carrying the token, for the dialects that expect them.

        iikoRMS takes the token as ``?key=``; iikoWeb takes a bearer header and
        its own front end sends no such parameter. Adding one to every URL was a
        deviation from the request we know works, and it puts a session token in
        URLs and access logs for no benefit. It is now sent only to resto-style
        paths, or when the authenticating dialect actually asked for it.
        """
        if not self.token:
            return {}
        wants_key = bool(self.dialect and self.dialect.get("token_param") == "key")
        if wants_key or path.startswith("/resto"):
            return {"key": self.token}
        return {}

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
        p.update(self._auth_params(path))

        # Browser identity first, then auth, then a correlation id unique to
        # this call — the portal issues a fresh one per XHR, and reusing a
        # single id across a whole backfill would look nothing like a browser.
        headers: Dict[str, str] = dict(_browser_headers(self.cfg))
        headers.update(self._auth_headers())
        headers["X-Correlation-Id"] = str(uuid.uuid4())
        if json_body is not None:
            # requests sets this itself when json= is used; stated explicitly so
            # the header is present even on an empty-dict body.
            headers["Content-Type"] = "application/json"

        return self.session.request(
            method,
            url,
            params=p or None,
            # json= (not data=) so requests serialises and encodes the payload.
            json=json_body,
            headers=headers,
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

        preset_id_now = (format_args or {}).get("preset_id")
        for template in _ordered_candidates(pinned, remembered, catalogue):
            # A preset the server has already disowned will not answer under any
            # payload, so skip every path that names it — including the literal
            # one baked into the catalogue.
            if self.preset_missing and preset_id_now and (
                    "{preset_id}" in template or str(preset_id_now) in template):
                attempts.append(f"{template}: skipped (preset reported missing)")
                continue
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
        chunks = list(_chunk_range(date_from, date_to, self.cfg.chunk_days))
        failures: List[str] = []
        empty_chunks = 0

        for chunk_start, chunk_end in chunks:
            self.empty_window = False
            try:
                rows = self._fetch_chunk(chunk_start, chunk_end)
            except IikoError as exc:
                # One bad window must not discard the rest of the year. A
                # backfill that reaches September is worth having even if
                # February refuses; the failures are collected and reported
                # only if nothing at all came back.
                failures.append(f"{chunk_start}..{chunk_end}: {exc}")
                log.warning("iiko: chunk %s..%s failed: %s",
                            chunk_start, chunk_end, str(exc)[:200])
                continue
            if rows:
                frames.append(pd.DataFrame(rows))
            else:
                empty_chunks += 1
                log.info("iiko: %s..%s returned no rows", chunk_start, chunk_end)
            # Be a polite API citizen on multi-month backfills.
            time.sleep(0.2)

        if not frames:
            if failures and len(failures) == len(chunks):
                raise IikoError(
                    f"Every window from {date_from} to {date_to} failed.\n  "
                    + "\n  ".join(failures[:4])
                    + (f"\n  … and {len(failures) - 4} more" if len(failures) > 4 else "")
                )
            # No failures, just nothing to report: an account whose history
            # starts later than the requested range, for instance.
            log.warning("iiko: %s..%s produced no rows across %d windows "
                        "(%d reported empty). If the venue traded in this period, "
                        "check IIKO_PRESET_ID and IIKO_STORE_IDS.",
                        date_from, date_to, len(chunks), empty_chunks)
            return _empty_sales_frame()

        if failures:
            log.warning("iiko: %d of %d windows failed but %d returned data; "
                        "keeping what arrived", len(failures), len(chunks), len(frames))
        raw = pd.concat(frames, ignore_index=True)
        out = normalise_olap_frame(raw)

        # Rows arrived but none survived normalisation — that is a parser
        # problem, not an empty period, and it must not be reported as success.
        if len(raw) and out.empty:
            sample = raw.head(3).to_dict(orient="records")
            _dump_response({"columns": list(raw.columns), "sample_rows": sample},
                           "rows fetched but none normalised")
            log.error(
                "parsed %d rows from iiko but none had a usable date/tender. "
                "Columns seen: %s. Raw sample saved to %s",
                len(raw), list(raw.columns)[:15], RAW_DUMP_PATH,
            )
            raise IikoError(
                f"Fetched {len(raw)} rows but none could be read as sales. "
                f"Columns seen: {list(raw.columns)[:15]}. "
                f"The raw response was saved to {RAW_DUMP_PATH} — send that file "
                "so the parser can be matched to the real shape."
            )
        return out

    def _fetch_chunk(self, date_from: dt.date, date_to: dt.date) -> List[Dict[str, Any]]:
        assert self.dialect
        log_lines: List[str] = []

        # 1) Run the saved preset. This is the path the portal itself uses:
        #    POST /api/olap/fetch/{preset_id}/grouped-table
        preset_id = self.resolve_preset_id()
        if preset_id:
            params = {"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat()}
            bodies = _preset_bodies(date_from, date_to, self.cfg)
            # A payload shape proven on an earlier chunk leads, so a year-to-date
            # backfill does not re-walk the whole list for every chunk.
            known = _meta_get("olap_body")
            if known:
                bodies.sort(key=lambda nb: 0 if nb[0] == known else 1)

            def preset_probe(path: str) -> Tuple[bool, Any, str]:
                """Try each payload shape against one path.

                The reported failure is the FIRST substantive one — the reply to
                the payload we most expect to work. An earlier version reported
                the last attempt instead, which was a deliberately empty body
                sent as a final long shot; its "Request data not found" reply
                masked why the real payload had been refused.
                """
                first_error: Optional[str] = None

                def remember(reason: str) -> None:
                    nonlocal first_error
                    if first_error is None:
                        first_error = reason

                for name, body in bodies:
                    resp = self._request("POST", path, json_body=body)
                    if body and _is_no_data(resp.status_code, resp.text):
                        # The address and the payload are both fine; this window
                        # simply holds no trading. An empty result, not a failure.
                        log.info("iiko: no trading data in window %s..%s",
                                 date_from, date_to)
                        self.body_shape = name
                        self.empty_window = True
                        return True, [], ""
                    if body and _is_preset_missing(resp.status_code, resp.text):
                        # The path's preset is gone. No payload will fix that,
                        # so stop reshaping the body and let the caller move on
                        # to the preset-free route.
                        reason = (f"HTTP {resp.status_code} {resp.text[:120]} "
                                  "— the preset in the path does not exist")
                        log.warning("iiko: %s", reason)
                        self.preset_missing = True
                        return False, None, reason
                    if resp.status_code in _ROUTE_MISS:
                        remember(f"HTTP {resp.status_code} (no such route)")
                        continue
                    ok, rows, why = _read_rows(resp)
                    if ok:
                        if name != known:
                            _meta_set("olap_body", name)
                            self._note(f"payload shape accepted: {name}")
                            log.info("iiko: payload shape '%s' accepted", name)
                        self.body_shape = name
                        return True, rows, ""
                    remember(f"payload '{name}': {why}")
                    log.info("iiko: payload '%s' rejected -> %s", name, why)

                # Long shots, tried only after every real payload was refused.
                for method, kwargs in (("POST", {"params": params, "json_body": {}}),
                                       ("GET", {"params": params})):
                    resp = self._request(method, path, **kwargs)
                    if resp.status_code in _ROUTE_MISS:
                        continue
                    ok, rows, why = _read_rows(resp)
                    if ok:
                        self.body_shape = f"{method.lower()}-params"
                        return True, rows, ""

                return False, None, first_error or "no payload accepted"

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
        resto_body = _olap_body(date_from, date_to, self.cfg.restaurant)

        def query_probe(path: str) -> Tuple[bool, Any, str]:
            # An /api/olap/fetch/... address expects the grouped-table payload;
            # the resto-style endpoints expect the reportType/filters shape.
            # Sending the wrong one earns a 400 that looks like a dead route.
            if "/olap/fetch/" not in path:
                return _read_rows(self._request("POST", path, json_body=resto_body))
            # Same two payload variants as the preset route: the full captured
            # field set, then the minimal one if the optional aggregates are
            # refused. A preset-free URL still needs a well-formed body.
            first_error: Optional[str] = None
            for name, body in _preset_bodies(date_from, date_to, self.cfg)[:2]:
                resp = self._request("POST", path, json_body=body)
                if resp.status_code in _ROUTE_MISS:
                    return False, None, f"HTTP {resp.status_code} (no such route)"
                if _is_no_data(resp.status_code, resp.text):
                    log.info("iiko: no trading data in window %s..%s",
                             date_from, date_to)
                    self.body_shape = name
                    self.empty_window = True
                    return True, [], ""
                ok, rows, why = _read_rows(resp)
                if ok:
                    self.body_shape = name
                    return True, rows, ""
                if first_error is None:
                    first_error = f"payload '{name}': {why}"
            return False, None, first_error or "no payload accepted"

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

        hint = (
            "\n\nThe server reported that the saved report behind the preset id"
            f" does not exist. IIKO_PRESET_ID is currently"
            f" '{preset_id}'. Either it was deleted in iikoWeb, or it belongs to"
            " another account — open the report in the portal and copy the id"
            " from the URL. The preset-free route /api/olap/fetch/grouped-table"
            " was tried as well and did not answer."
            if self.preset_missing else
            "\n\nRun `python app.py doctor` to see the full probe. If a path"
            " answered 200 but no rows were recognised, the response shape is new:"
            " capture it from DevTools so _extract_rows can be taught to read it."
        )
        raise IikoError(
            f"No OLAP endpoint answered for {date_from}..{date_to}. Tried:\n  "
            + "\n  ".join(log_lines) + hint
        )


# --- The grouped-table request payload ---------------------------------------
# Reproduces the portal's own POST to /api/olap/fetch/{preset_id}/grouped-table
# verbatim. Only dateFrom / dateTo inside the OpenDate.Typed filter vary.
#
# Every filter carries the server's full key set — valueMin, valueMax,
# valueList, includeLeft, includeRight, inclusiveList — even where the values
# are null or empty. An earlier, browser-truncated copy of this payload omitted
# them and the server answered 400, so the keys are not decorative: they are
# built here explicitly rather than trimmed to the ones that look meaningful.
GROUPED_GROUP_FIELDS: List[str] = [
    "OpenDate.Typed",                 # the business date
    "RestorauntGroup",                # venue
    "PayTypes.Combo",                 # the tender string the channel engine reads
]
GROUPED_DATA_FIELDS: List[str] = [
    "UniqOrderId.OrdersCount",        # Заказы            -> order_count
    "DishSumInt",                     # Сумма без скидки  -> gross_sales
    "DishDiscountSumInt",             # Сумма со скидкой  -> net_sales
    "ProductCostBase.Percent",        # Себестоимость %   -> cogs_pct
    "ProductCostBase.ProductCost",    # Себестоимость     -> cogs (an amount)
    "DishDiscountSumInt.average",     # СрЧек             -> avg check (recomputed)
]

# The four fields the dashboard cannot work without. Requested alone as a
# retry: a preset that rejects ProductCostBase.Percent or the .average
# aggregate answers 400, and losing COGS-as-a-ratio costs nothing because the
# amount is in this set, while the average check is recomputed anyway.
GROUPED_DATA_FIELDS_MINIMAL: List[str] = [
    "UniqOrderId.OrdersCount",
    "DishSumInt",
    "DishDiscountSumInt",
    "ProductCostBase.ProductCost",
]

# Rows the report must never count: deleted orders and write-offs.
NOT_DELETED_FILTER_FIELDS: List[str] = ["OrderDeleted", "DeletedWithWriteoff"]


def _date_range_filter(field: str, date_from: dt.date, date_to: dt.date) -> Dict[str, Any]:
    """A date_range filter with the complete key set the server expects."""
    return {
        "field": field,
        "filterType": "date_range",
        "dateFrom": date_from.isoformat(),
        "dateTo": date_to.isoformat(),
        "valueMin": None,
        "valueMax": None,
        "valueList": [],
        "includeLeft": True,
        "includeRight": True,      # the window is inclusive at both ends
        "inclusiveList": True,
    }


def _value_list_filter(field: str, values: Sequence[str]) -> Dict[str, Any]:
    """A value_list filter with the complete key set the server expects."""
    return {
        "field": field,
        "filterType": "value_list",
        "dateFrom": None,
        "dateTo": None,
        "valueMin": None,
        "valueMax": None,
        "valueList": list(values),
        "includeLeft": True,
        "includeRight": False,     # note: false here, unlike the date filter
        "inclusiveList": True,
    }


def _grouped_table_body(date_from: dt.date, date_to: dt.date,
                        cfg: Config = CFG,
                        data_fields: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    """Build the grouped-table payload, with the date window substituted in.

    `data_fields` defaults to the full captured set; pass
    GROUPED_DATA_FIELDS_MINIMAL to retry without the optional aggregates.
    """
    return {
        # A list either way: ints for numeric ids, strings for UUIDs, [] when
        # unset. Never None — the server reads a missing list as malformed.
        "storeIds": list(cfg.store_ids) if cfg.store_ids else [],
        "olapType": "SALES",
        "groupFields": list(GROUPED_GROUP_FIELDS),
        "dataFields": list(data_fields if data_fields is not None else GROUPED_DATA_FIELDS),
        "calculatedFields": [],
        "filters": [
            # Dates are ISO YYYY-MM-DD, produced by date.isoformat().
            _date_range_filter("OpenDate.Typed", date_from, date_to),
            *(_value_list_filter(f, ["NOT_DELETED"]) for f in NOT_DELETED_FILTER_FIELDS),
        ],
        "includeVoidTransactions": _as_bool(cfg.include_void),
        "includeNonBusinessPaymentTypes": _as_bool(cfg.include_non_business),
    }


def _preset_bodies(date_from: dt.date, date_to: dt.date,
                   cfg: Config = CFG) -> List[Tuple[str, Dict[str, Any]]]:
    """(name, body) candidates for a preset-scoped OLAP fetch, best first.

    The captured grouped-table payload leads. The remaining shapes are kept only
    so a differently-configured host still has a chance; the winning name is
    cached in `meta` so this list is walked once, not once per chunk.
    """
    f, t = date_from.isoformat(), date_to.isoformat()
    out: List[Tuple[str, Dict[str, Any]]] = [
        ("grouped-table", _grouped_table_body(date_from, date_to, cfg)),
        ("grouped-table-minimal",
         _grouped_table_body(date_from, date_to, cfg, GROUPED_DATA_FIELDS_MINIMAL)),
    ]
    out += [
        ("dateFrom/dateTo", {"dateFrom": f, "dateTo": t}),
        ("from/to", {"from": f, "to": t}),
        ("startDate/endDate", {"startDate": f, "endDate": t}),
        ("period", {"period": {"from": f, "to": t}}),
        ("resto-filters", {"filters": {"OpenDate.Typed": {
            "filterType": "DateRange", "periodType": "CUSTOM", "from": f, "to": t}}}),
        # No empty-body candidate: it cannot succeed on a grouped-table route,
        # and the server answers it with the very same "Request data not found"
        # that a deleted preset produces — which made a bad payload look like a
        # missing report and sent the diagnosis down the wrong path.
    ]
    return out


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
        _dump_response(data, "unparsed")
        return False, None, (
            "no recognisable rows in response; raw JSON saved to "
            f"{RAW_DUMP_PATH.name} for inspection"
        )
    return True, rows, ""


RAW_DUMP_PATH = DATA_DIR / "last_olap_response.json"


def _dump_response(data: Any, reason: str) -> None:
    """Persist a response the parser could not use.

    Guessing at an unseen JSON shape from a log line costs far more than a file
    on disk, so the payload is kept whenever parsing produces nothing usable.
    """
    try:
        RAW_DUMP_PATH.write_text(
            json.dumps({"reason": reason,
                        "captured_at": dt.datetime.now().isoformat(timespec="seconds"),
                        "response": data},
                       ensure_ascii=False, indent=2)[:4_000_000],
            encoding="utf-8",
        )
        log.warning("saved unparsed OLAP response (%s) to %s", reason, RAW_DUMP_PATH)
    except Exception as exc:                                    # pragma: no cover
        log.warning("could not save raw response: %s", exc)


def _describe_shape(data: Any, depth: int = 0) -> str:
    """A one-line sketch of a JSON structure, for logs."""
    if depth > 3:
        return "…"
    if isinstance(data, dict):
        inner = ", ".join(f"{k}: {_describe_shape(v, depth + 1)}" for k, v in list(data.items())[:8])
        more = ", …" if len(data) > 8 else ""
        return "{" + inner + more + "}"
    if isinstance(data, list):
        return f"[{len(data)} x {_describe_shape(data[0], depth + 1)}]" if data else "[]"
    return type(data).__name__


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


# Canonical names that identify a row as an actual measured row rather than a
# grouping node. A leaf must carry at least one of these to count.
MEASURE_CANON = {"order_count", "gross_sales", "net_sales", "cogs",
                 "cogs_pct_src", "avg_check_src"}
DIMENSION_CANON = {"business_date", "payment_type", "venue"}


def _canon_of(key: Any) -> Optional[str]:
    """The canonical field this response key maps to, if any."""
    return FIELD_ALIASES.get(_norm_key(key))


def _harvest_leaves(node: Any, inherited: Dict[str, Any],
                    out: List[Dict[str, Any]], depth: int = 0) -> None:
    """Walk a grouped payload and collect its leaf rows.

    Key-name agnostic on purpose. Earlier versions looked for containers called
    "children"/"groups"/"rows" and fell back to returning the group nodes
    themselves when the name did not match — which is how 29 parent nodes, each
    still holding a `children` list, reached pandas and got mistaken for data.

    This walks every nested list or dict instead, carries each level's scalar
    values down to its descendants (the date and tender live on the group nodes,
    the measures on the leaves), and emits only the deepest nodes. Emitting only
    leaves is what prevents an OLAP subtotal being counted twice: a parent that
    carries both an aggregate and children is descended into, not recorded.
    """
    if depth > 12:                                   # pathological nesting guard
        return
    if isinstance(node, list):
        for item in node:
            _harvest_leaves(item, inherited, out, depth)
        return
    if not isinstance(node, dict):
        return

    scalars = {k: v for k, v in node.items()
               if not isinstance(v, (dict, list)) and k not in ("id", "uuid")}
    context = {**inherited, **scalars}

    nested = [v for v in node.values()
              if (isinstance(v, dict) and v)
              or (isinstance(v, list) and any(isinstance(i, (dict, list)) for i in v))]

    if nested:
        for child in nested:
            _harvest_leaves(child, context, out, depth + 1)
    elif context:
        out.append(context)


def _rows_from_grouped(data: Any) -> Optional[List[Dict[str, Any]]]:
    """Flatten a grouped payload, keeping only rows that carry a measure."""
    leaves: List[Dict[str, Any]] = []
    _harvest_leaves(data, {}, leaves)
    if not leaves:
        return None
    rows = [r for r in leaves if any(_canon_of(k) in MEASURE_CANON for k in r)]
    if not rows:
        return None
    log.debug("grouped payload flattened to %d leaf rows (from %d leaves)",
              len(rows), len(leaves))
    return rows


def _has_nested_container(item: Dict[str, Any]) -> bool:
    """True when a dict holds a child list of dicts, or a non-empty sub-dict."""
    for val in item.values():
        if isinstance(val, dict) and val:
            return True
        if isinstance(val, list) and any(isinstance(i, (dict, list)) for i in val):
            return True
    return False


def _flat_dicts(items: Sequence[Any]) -> bool:
    """True when every entry is a dict holding no nested container."""
    return (bool(items) and all(isinstance(x, dict) for x in items)
            and not any(_has_nested_container(x) for x in items))


def _looks_like_rows(items: Sequence[Any]) -> bool:
    """True when a list of dicts is flat measured rows, not grouping nodes.

    Carrying a measure is not enough: an OLAP group node often holds its own
    subtotal *and* its children. Treating such a node as a row would record the
    subtotal and silently discard everything beneath it, so anything with a
    nested container is sent down the grouped path instead.
    """
    if not items or not all(isinstance(x, dict) for x in items):
        return False
    if any(_has_nested_container(x) for x in items):
        return False
    return any(_canon_of(k) in MEASURE_CANON for x in items for k in x)


# ------------------------------------------------------------------------------
#  The iikoWeb grouped-table response
# ------------------------------------------------------------------------------
#  Verified shape:
#
#    {"result": {
#       "headers": [{"field": "OpenDate.Typed"}, {"field": "RestorauntGroup"}, …],
#       "rows": [
#         {"field0": {"type": "DATE", "value": "2026-09-01"},
#          "field3": {"type": "AMOUNT", "value": 224}, …,      <- date subtotal
#          "children": [
#            {"field1": {"type": "STRING", "value": "Նանի"},   <- venue subtotal
#             "children": [
#               {"field2": {"type": "STRING", "value": "Glovo"},
#                "field3": …, "field4": …}                     <- the actual row
#             ]}]}]}}
#
#  Two things make this unreadable by any generic walker:
#
#  * Columns are POSITIONAL. "field3" means nothing on its own — it is the
#    fourth entry of `headers`. The index-to-name map has to be built first.
#  * Every cell is wrapped as {"type": …, "value": …} rather than being a bare
#    scalar, so a walker looking for scalar values finds none.
#
#  Group nodes also carry their own subtotals alongside their children, so only
#  the deepest nodes are emitted or every figure would be counted twice.

_CELL_CONTAINER_KEYS = ("children", "rows", "subRows", "groups")


def _header_field_map(headers: Sequence[Any]) -> Dict[str, str]:
    """Build {"field0": "OpenDate.Typed", …} from the ordered header list."""
    out: Dict[str, str] = {}
    for index, header in enumerate(headers):
        if isinstance(header, dict):
            name = (header.get("field") or header.get("name") or header.get("id")
                    or header.get("title") or header.get("key"))
        else:
            name = header
        if name:
            out[f"field{index}"] = str(name)
    return out


def _cell_value(cell: Any) -> Any:
    """Unwrap {"type": "MONEY", "value": 271310} down to the value."""
    if isinstance(cell, dict):
        if "value" in cell:
            return cell["value"]
        return None
    return cell


def _find_headers_and_rows(data: Any, depth: int = 0) -> Optional[Tuple[List[Any], List[Any]]]:
    """Locate the object carrying both `headers` and `rows`, at any depth."""
    if depth > 6:
        return None
    if isinstance(data, dict):
        headers, rows = data.get("headers"), data.get("rows")
        if isinstance(headers, list) and headers and isinstance(rows, list):
            return headers, rows
        for val in data.values():
            found = _find_headers_and_rows(val, depth + 1)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = _find_headers_and_rows(item, depth + 1)
            if found:
                return found
    return None


def _walk_grouped_rows(node: Any, inherited: Dict[str, Any], field_map: Dict[str, str],
                       out: List[Dict[str, Any]], depth: int = 0) -> None:
    """Descend the row tree, emitting only leaves with ancestors' values merged."""
    if depth > 12:
        return
    if isinstance(node, list):
        for item in node:
            _walk_grouped_rows(item, inherited, field_map, out, depth)
        return
    if not isinstance(node, dict):
        return

    row = dict(inherited)
    for key, cell in node.items():
        name = field_map.get(key)
        if not name:
            continue
        value = _cell_value(cell)
        if value is not None:
            row[name] = value

    children: List[Any] = []
    for key in _CELL_CONTAINER_KEYS:
        val = node.get(key)
        if isinstance(val, list) and val:
            children = val
            break

    if children:
        # A group node also holds its own subtotal; descend rather than record it.
        #
        # Only dimensions are handed down. If the parent's aggregate measures
        # were inherited too, a leaf that happens to omit one column would
        # silently pick up the whole group's subtotal for it — a single missing
        # cell would then inflate that day's revenue by the day's own total.
        passed_down = {k: v for k, v in row.items()
                       if _canon_of(k) not in MEASURE_CANON}
        _walk_grouped_rows(children, passed_down, field_map, out, depth + 1)
    elif row:
        out.append(row)


def _rows_from_iiko_grouped_table(data: Any) -> Optional[List[Dict[str, Any]]]:
    """Read the portal's grouped-table response into flat, named rows."""
    found = _find_headers_and_rows(data)
    if not found:
        return None
    headers, rows = found
    field_map = _header_field_map(headers)
    if not field_map:
        return None

    leaves: List[Dict[str, Any]] = []
    _walk_grouped_rows(rows, {}, field_map, leaves, 0)
    if not leaves:
        return [] if not rows else None

    log.info("grouped-table: %d header columns, %d top-level rows -> %d leaf rows",
             len(field_map), len(rows), len(leaves))
    log.debug("grouped-table columns: %s", list(field_map.values()))
    return leaves


def _extract_rows(data: Any) -> Optional[List[Dict[str, Any]]]:
    """Locate the measured rows inside any shape iiko returns.

    Order matters: the portal's own headers+rows tree is unambiguous and is read
    first. A columns+rows table comes next, then flat measured rows, and only
    then the generic grouped-tree walker.
    """
    # 0) the verified iikoWeb grouped-table response
    iiko = _rows_from_iiko_grouped_table(data)
    if iiko is not None:
        return iiko

    # 1) columns + array-of-arrays
    if isinstance(data, dict):
        table = _rows_from_table(data)
        if table is not None:
            return table

    # 2) an already-flat list of measured rows
    if isinstance(data, list) and _looks_like_rows(data):
        return list(data)
    if isinstance(data, dict):
        for key in _ROW_KEYS:
            val = data.get(key)
            if isinstance(val, list) and _looks_like_rows(val):
                return list(val)

    # 3) a grouped tree, under any key names at all
    grouped = _rows_from_grouped(data)
    if grouped is not None:
        return grouped

    # 4) a flat list of dicts whose field names we do not recognise. Handing it
    #    on lets the header sniffing in normalise_olap_frame try to make sense
    #    of a renamed preset. Nodes with children are excluded above, so this
    #    cannot resurrect the group-node bug.
    if isinstance(data, list) and _flat_dicts(data):
        return list(data)
    if isinstance(data, dict):
        for key in _ROW_KEYS:
            val = data.get(key)
            if isinstance(val, list) and _flat_dicts(val):
                return list(val)
            if isinstance(val, dict):
                nested = _extract_rows(val)
                if nested is not None:
                    return nested

    # 5) an empty but well-formed result is success, not a parse failure
    if isinstance(data, dict):
        for key in _ROW_KEYS:
            if isinstance(data.get(key), list) and not data[key]:
                return []
    if isinstance(data, list) and not data:
        return []
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

CANON_COLUMNS = ["business_date", "payment_type", "venue",
                 "order_count", "gross_sales", "net_sales", "cogs"]


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


def _to_ratio(val: Any) -> float:
    """Read a percentage that may arrive as 28.5 or as 0.285, returning 0.285.

    Restaurant COGS sits around 20–40%, so a value above 1.5 is percentage
    points and anything below it is already a fraction. The ambiguous band
    (a genuine COGS under 1.5%) does not occur in food service.
    """
    num = _to_number(val)
    if num <= 0:
        return 0.0
    return num / 100.0 if num > 1.5 else num


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


def _scalar_values(series: Any) -> List[Any]:
    """Non-empty scalar values only.

    A column holding lists or dicts is structural leftovers, never a dimension.
    Letting one through is how a `children` column was once nominated as the
    tender field, which silently turned a whole year of data into nothing.
    """
    out: List[Any] = []
    for v in series.tolist():
        if isinstance(v, (list, dict, set, tuple)):
            return []
        if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
            continue
        out.append(v)
    return out


def _guess_date_column(raw: pd.DataFrame, exclude: Iterable[str] = ()) -> Optional[str]:
    """Find the column that behaves like a business date."""
    best, best_rate = None, 0.0
    sample = raw.head(200)
    for col in raw.columns:
        if col in set(exclude):
            continue
        vals = _scalar_values(sample[col])
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
        scalars = _scalar_values(sample[col])
        if not scalars:
            continue
        vals = [str(v) for v in scalars]
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

    # The payload requests COGS both as an amount (ProductCostBase.ProductCost)
    # and as a ratio (ProductCostBase.Percent). The amount is authoritative; the
    # ratio only fills in rows that arrive without one.
    #
    # The base matters: complimentary checks have net sales of zero, so
    # net x percent would cost every giveaway at zero and the Compliments tab
    # would show hundreds of checks costing nothing. Those rows are costed off
    # gross (menu value) instead, which is what actually left the kitchen.
    if "cogs_pct_src" in df.columns:
        pct = df["cogs_pct_src"].map(_to_ratio)
        missing = df["cogs"] <= 0
        base = df["net_sales"].where(df["net_sales"] > 0, df["gross_sales"])
        df.loc[missing, "cogs"] = (base[missing] * pct[missing]).fillna(0.0)
        if missing.any():
            log.debug("normalise: reconstructed COGS from percentage for %d rows",
                      int(missing.sum()))

    df["order_count"] = df["order_count"].round().astype("int64")
    df["channel"] = df["payment_type"].map(classify_channel)
    df = df[df["business_date"].notna()].copy()

    df["venue"] = df["venue"].fillna("").astype(str).str.strip()
    out = (
        df.groupby(["business_date", "payment_type", "venue", "channel"], as_index=False)[
            ["order_count", "gross_sales", "net_sales", "cogs"]
        ].sum()
    )
    return out[["business_date", "payment_type", "venue", "channel",
                "order_count", "gross_sales", "net_sales", "cogs"]]


# ==============================================================================
#  SECTION 6 — LOCAL CACHE (SQLite)
# ==============================================================================
#  The dashboard never queries iiko on user interaction. A background sync keeps
#  a local SQLite mirror of the year-to-date Sales OLAP report, and every filter,
#  comparison and export is served from that mirror. This keeps the UI instant
#  and the API load to one small request per morning.

#  The venue is part of the primary key. The report groups by RestorauntGroup,
#  so a key of (date, payment_type) alone would let two venues' rows for the
#  same day and tender overwrite one another — silently dropping a whole
#  restaurant's trade from every total.
SCHEMA = """
CREATE TABLE IF NOT EXISTS sales_daily (
    business_date TEXT    NOT NULL,
    payment_type  TEXT    NOT NULL,
    venue         TEXT    NOT NULL DEFAULT '',
    channel       TEXT    NOT NULL,
    order_count   INTEGER NOT NULL DEFAULT 0,
    gross_sales   REAL    NOT NULL DEFAULT 0,
    net_sales     REAL    NOT NULL DEFAULT 0,
    cogs          REAL    NOT NULL DEFAULT 0,
    PRIMARY KEY (business_date, payment_type, venue)
);
CREATE INDEX IF NOT EXISTS ix_sales_date    ON sales_daily (business_date);
CREATE INDEX IF NOT EXISTS ix_sales_channel ON sales_daily (channel);
CREATE INDEX IF NOT EXISTS ix_sales_venue   ON sales_daily (venue);

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


def _needs_venue_migration(conn: sqlite3.Connection) -> bool:
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(sales_daily)")}
    except sqlite3.Error:
        return False
    return bool(cols) and "venue" not in cols


def init_db() -> None:
    with _db_lock, _connect() as conn:
        if _needs_venue_migration(conn):
            # The old key was (date, payment_type). Where more than one venue
            # traded, its rows already overwrote each other, so the contents
            # cannot be trusted and adding a column would preserve the damage.
            # Drop it; the next sync refills from iiko, which is authoritative.
            log.warning("cache predates the venue key — dropping it for a clean re-sync")
            conn.executescript("DROP TABLE IF EXISTS sales_daily;")
            conn.execute("DELETE FROM meta WHERE key IN ('last_sync_date','last_sync_at')")
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
                str(getattr(r, "venue", "") or ""),
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
            "(business_date, payment_type, venue, channel, order_count, gross_sales, net_sales, cogs) "
            "VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(business_date, payment_type, venue) DO UPDATE SET "
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
    if "venue" not in df.columns:
        df["venue"] = ""
    df["venue"] = df["venue"].fillna("").astype(str)
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

def _history_start(today: dt.date, cfg: Config = CFG) -> dt.date:
    """The first date a full rebuild asks for.

    Defaults to DEFAULT_HISTORY_START (the day the venue opened) rather than
    1 January, so a rebuild recovers the whole history in one pass.
    """
    raw = (cfg.history_start or "").strip()
    if raw:
        parsed = _to_date(raw)
        if parsed:
            return parsed
        log.warning("IIKO_HISTORY_START=%r is not a date; using 1 January", raw)
    return dt.date(today.year, 1, 1)


def sync(full: bool = False, client: Optional[IikoClient] = None) -> Dict[str, Any]:
    """Refresh the local mirror. Returns a small report for the UI / CLI."""
    init_db()
    client = client or IikoClient()
    today = dt.date.today()
    lo, hi, _ = cache_span()

    # Read settings from the client's own config, not the module-level one:
    # a client built with a different Config (a test, a second venue) must not
    # silently pick up the global window.
    cfg = client.cfg
    if full or lo is None:
        start = _history_start(today, cfg)
        mode = f"full history from {start:%d %b %Y}"
    else:
        tail = dt.timedelta(days=max(0, cfg.incremental_tail_days))
        start = min(hi, today) - tail
        start = max(start, _history_start(today, cfg))
        mode = f"incremental (last {cfg.incremental_tail_days} days)"

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
    """The comparison window preceding [lo, hi], covering the SAME days.

    Month-over-month is the case that matters. On the 29th, "this month" is
    1–29 September; comparing it against the whole of August would put 29 days
    of trading against 31 and report a fall that is purely calendar arithmetic.
    The prior window is therefore 1–29 August: the same day-of-month span.

    Where the earlier month is shorter (31 March against February), the window
    is clamped to that month's last day and is necessarily shorter; nothing can
    invent a 31st of February, and clamping is the smaller distortion.
    """
    if mode == "month":
        first_of_this = lo.replace(day=1)
        prev_month_end = first_of_this - dt.timedelta(days=1)
        prev_start = prev_month_end.replace(day=1)
        span = (hi - lo).days                      # days after the start
        prev_end = min(prev_start + dt.timedelta(days=span), prev_month_end)
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


def drilldown_with_comparison(cur: pd.DataFrame, prev: pd.DataFrame,
                              grain: str) -> pd.DataFrame:
    """The drill-down table, each period set against its opposite number.

    Periods are matched by position from the start of each window, not by date:
    the first day of this month against the first day of last month, week 1
    against week 1. Because previous_period() now returns a window covering the
    same days, position and calendar day line up, and the pairing stays honest
    when a month has a different number of days.
    """
    base = drilldown_table(cur, grain)
    if base.empty:
        return base

    cols = ["Δ Net", "Δ Net %", "Δ Orders", "Δ Orders %"]
    if prev is None or prev.empty:
        for col in cols:
            base[col] = None
        return base

    prior = drilldown_table(prev, grain)
    # Both tables are newest-first; reverse so index 0 is each window's start.
    cur_asc = base.iloc[::-1].reset_index(drop=True)
    prev_asc = prior.iloc[::-1].reset_index(drop=True)

    deltas: List[Dict[str, Any]] = []
    for i in range(len(cur_asc)):
        if i < len(prev_asc):
            p_net = float(prev_asc.loc[i, "Net Sales"])
            p_ord = float(prev_asc.loc[i, "Orders"])
            c_net = float(cur_asc.loc[i, "Net Sales"])
            c_ord = float(cur_asc.loc[i, "Orders"])
            deltas.append({
                "Δ Net": c_net - p_net,
                "Δ Net %": (c_net - p_net) / abs(p_net) if p_net else None,
                "Δ Orders": c_ord - p_ord,
                "Δ Orders %": (c_ord - p_ord) / abs(p_ord) if p_ord else None,
            })
        else:
            # No opposite number — the prior window is shorter (a clamped month).
            deltas.append({c: None for c in cols})

    delta_df = pd.DataFrame(deltas)
    merged = pd.concat([cur_asc, delta_df], axis=1)
    return merged.iloc[::-1].reset_index(drop=True)


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
    --paper:       #F6F7F9;
    --paper-warm:  #FBFBF9;
    --surface:     #FFFFFF;
    --surface-2:   #F1F3F7;
    --line:        #E3E7EE;
    --line-soft:   #EDF0F5;
    --ink:         #0F172A;
    --muted:       #5A6678;
    --faint:       #8A94A6;
    --gold:        #A8801A;
    --gold-soft:   #C9A227;
    --gold-wash:   #FBF3DC;
    --emerald:     #047857;
    --emerald-wash:#E6F5EF;
    --crimson:     #BE1E2D;
    --crimson-wash:#FCEBEC;
    --shadow-sm:   0 1px 2px rgba(15,23,42,.04), 0 1px 3px rgba(15,23,42,.05);
    --shadow-md:   0 2px 4px rgba(15,23,42,.04), 0 6px 16px rgba(15,23,42,.06);
  }

  .stApp {
    background:
      radial-gradient(1100px 520px at 12% -12%, #FFFFFF 0%, rgba(255,255,255,0) 62%),
      var(--paper);
    color: var(--ink);
    font-family: 'Inter', 'Noto Sans Armenian', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  }
  .block-container { padding: .6rem 1.6rem 4rem; max-width: 1560px; }
  #MainMenu, footer, header [data-testid="stToolbar"] { visibility: hidden; }
  header[data-testid="stHeader"] { background: transparent; height: 0; }
  [data-testid="stDecoration"] { display: none; }

  h1, h2, h3, h4 { color: var(--ink); font-weight: 700; letter-spacing: -0.015em; }

  /* ---------- Masthead ---------- */
  .nani-head {
    display: flex; align-items: center; gap: 18px; flex-wrap: wrap;
    padding: 14px 20px; margin-bottom: 18px;
    background: var(--surface);
    border: 1px solid var(--line);
    border-left: 3px solid var(--gold-soft);
    border-radius: 16px;
    box-shadow: var(--shadow-sm);
  }
  .nani-head img { height: 50px; width: auto; border-radius: 10px; display: block; }
  .nani-head .title {
    font-family: 'Playfair Display', Georgia, serif;
    font-size: 1.58rem; font-weight: 700; color: var(--ink); line-height: 1.15; margin: 0;
  }
  .nani-head .spacer { flex: 1 1 auto; }
  .nani-head .stamp { text-align: right; font-size: .72rem; color: var(--faint); line-height: 1.6; }
  .nani-head .stamp b { color: var(--gold); font-weight: 700; }

  /* ---------- KPI tiles ----------
     Compact frame, larger figure: the number is the content, the card is only
     its mount. Padding is tight so a six-tile row stays on one line. */
  .kpi-grid {
    display: grid; gap: 10px; margin: 4px 0 18px;
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  }
  .kpi-grid.cols-6 { grid-template-columns: repeat(6, minmax(0, 1fr)); }
  .kpi-grid.cols-5 { grid-template-columns: repeat(5, minmax(0, 1fr)); }
  .kpi-grid.cols-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .kpi-grid.cols-3 { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .kpi-grid.cols-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }

  .kpi {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 12px 14px 11px;
    position: relative; overflow: hidden;
    box-shadow: var(--shadow-sm);
    transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
  }
  .kpi:hover {
    transform: translateY(-2px);
    box-shadow: var(--shadow-md);
    border-color: #D6DCE6;
  }
  .kpi::after {
    content: ''; position: absolute; left: 0; top: 10px; bottom: 10px; width: 3px;
    border-radius: 0 3px 3px 0;
    background: var(--gold-soft);
  }
  .kpi.accent-emerald::after { background: var(--emerald); }
  .kpi.accent-crimson::after { background: var(--crimson); }
  .kpi.accent-muted::after   { background: #C6CDD8; }

  .kpi .label {
    font-size: .625rem; text-transform: uppercase; letter-spacing: .13em;
    color: var(--faint); font-weight: 700; margin-bottom: 5px; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis;
  }
  .kpi .value {
    font-size: 1.72rem; font-weight: 700; color: var(--ink);
    line-height: 1.08; letter-spacing: -0.028em; font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }
  .kpi .value.sm { font-size: 1.44rem; }
  .kpi .sub { font-size: .66rem; color: var(--faint); margin-top: 4px; }

  .badge {
    display: inline-flex; align-items: center; gap: 4px; margin-top: 8px;
    padding: 2px 8px; border-radius: 999px;
    font-size: .68rem; font-weight: 700; font-variant-numeric: tabular-nums;
    line-height: 1.5;
  }
  .badge.up   { background: var(--emerald-wash); color: var(--emerald); border: 1px solid #BFE3D5; }
  .badge.down { background: var(--crimson-wash); color: var(--crimson); border: 1px solid #F3C8CC; }
  .badge.flat { background: var(--surface-2);    color: var(--muted);   border: 1px solid var(--line); }
  .badge .abs { color: var(--faint); font-weight: 500; }

  /* ---------- Section furniture ---------- */
  .sec-title {
    font-size: .70rem; text-transform: uppercase; letter-spacing: .16em;
    color: var(--gold); font-weight: 800; margin: 22px 0 10px;
    padding-bottom: 7px; border-bottom: 1px solid var(--line);
  }
  .panel {
    background: var(--surface); border: 1px solid var(--line);
    border-radius: 14px; padding: 14px 16px; margin-bottom: 14px;
    box-shadow: var(--shadow-sm);
  }
  .note { font-size: .74rem; color: var(--faint); line-height: 1.6; }
  .chip {
    display: inline-block; padding: 3px 10px; border-radius: 999px;
    background: var(--gold-wash); border: 1px solid #EBDCAE;
    color: var(--gold); font-size: .7rem; font-weight: 700; margin-right: 6px;
  }

  /* ---------- Channel header ---------- */
  .ch-head { display: flex; align-items: center; gap: 14px; margin-bottom: 6px; }
  .ch-head img { height: 42px; width: auto; border-radius: 9px; }
  .ch-head .name { font-size: 1.2rem; font-weight: 700; color: var(--ink); }
  .ch-head .dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; }

  /* ---------- Streamlit control overrides ---------- */
  section[data-testid="stSidebar"] {
    background: var(--surface); border-right: 1px solid var(--line);
  }
  section[data-testid="stSidebar"] .block-container { padding-top: 1.2rem; }

  .stTabs [data-baseweb="tab-list"] {
    gap: 4px; background: transparent; border-bottom: 1px solid var(--line);
  }
  .stTabs [data-baseweb="tab"] {
    height: 40px; padding: 0 16px; background: transparent;
    color: var(--muted); font-weight: 600; font-size: .86rem;
    border-radius: 9px 9px 0 0;
  }
  .stTabs [aria-selected="true"] {
    background: var(--surface) !important; color: var(--gold) !important;
    border: 1px solid var(--line); border-bottom: 1px solid var(--surface);
  }

  div[data-baseweb="select"] > div,
  div[data-baseweb="input"]  > div,
  div[data-testid="stDateInput"] input {
    background-color: var(--surface) !important;
    border-color: var(--line) !important;
    color: var(--ink) !important;
  }
  div[data-baseweb="select"] svg { fill: var(--muted); }
  ul[data-baseweb="menu"], div[data-baseweb="popover"] > div {
    background-color: var(--surface) !important; color: var(--ink) !important;
  }
  li[data-baseweb="menu-item"]:hover { background-color: var(--surface-2) !important; }
  div[data-testid="stExpander"] details {
    background: var(--surface); border: 1px solid var(--line); border-radius: 10px;
  }
  div[data-testid="stExpander"] summary { color: var(--muted); }

  .stButton > button, .stDownloadButton > button {
    background: linear-gradient(135deg, #C9A227 0%, #A8801A 100%);
    color: #FFFFFF; border: none; border-radius: 10px;
    font-weight: 700; font-size: .82rem; padding: .5rem 1.1rem;
    box-shadow: var(--shadow-sm);
    transition: filter .15s ease;
  }
  .stButton > button:hover, .stDownloadButton > button:hover {
    filter: brightness(1.06); color: #FFFFFF;
  }

  /* ---------- Drill-down table ---------- */
  .tbl-wrap {
    overflow: auto; max-height: 540px;
    border: 1px solid var(--line); border-radius: 14px;
    background: var(--surface); margin-bottom: 12px;
    box-shadow: var(--shadow-sm);
    -webkit-overflow-scrolling: touch;
  }
  table.drill { width: 100%; border-collapse: collapse; font-size: .82rem; }
  table.drill thead th {
    position: sticky; top: 0; z-index: 2;
    background: var(--surface-2); color: var(--muted);
    font-size: .63rem; text-transform: uppercase; letter-spacing: .1em; font-weight: 800;
    padding: 10px 14px; text-align: right; white-space: nowrap;
    border-bottom: 1px solid var(--line);
  }
  table.drill thead th:first-child { text-align: left; }
  table.drill td {
    padding: 9px 14px; text-align: right; white-space: nowrap;
    color: var(--ink); font-variant-numeric: tabular-nums;
    border-bottom: 1px solid var(--line-soft);
  }
  table.drill td:first-child { text-align: left; font-weight: 600; }
  table.drill td.muted { color: var(--muted); }
  table.drill td.up   { color: var(--emerald); font-weight: 700; }
  table.drill td.down { color: var(--crimson); font-weight: 700; }
  table.drill td.flat { color: var(--faint); }
  table.drill tbody tr:hover td { background: var(--gold-wash); }
  table.drill tbody tr:last-child td { border-bottom: none; }
  table.drill td.sep, table.drill th.sep { border-left: 1px solid var(--line); }

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
    .kpi-grid.cols-4, .kpi-grid.cols-3 { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .kpi { padding: 11px 12px 10px; border-radius: 12px; }
    .kpi .value { font-size: 1.46rem; }
    .kpi .value.sm { font-size: 1.24rem; }
    .kpi .label { font-size: .58rem; letter-spacing: .09em; }
    .nani-head { padding: 12px 14px; gap: 12px; }
    .nani-head img { height: 38px; }
    .nani-head .title { font-size: 1.14rem; }
    .nani-head .stamp { text-align: left; width: 100%; }
    .stTabs [data-baseweb="tab"] { padding: 0 10px; font-size: .78rem; height: 38px; }
  }
  @media (max-width: 359px) {
    .kpi-grid, .kpi-grid.cols-6, .kpi-grid.cols-5,
    .kpi-grid.cols-4, .kpi-grid.cols-3, .kpi-grid.cols-2 { grid-template-columns: 1fr; }
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
    hoverlabel=dict(bgcolor=Palette.SURFACE, bordercolor=Palette.LINE,
                    font=dict(color=Palette.INK)),
)


def channel_mix_chart(mix: pd.DataFrame) -> Any:
    """One ring carrying both the money and the share for every channel.

    The amount and the percentage sit on the segment itself rather than in a
    legend, so the ring answers "how much" and "what share" without the eye
    having to travel to a second table.
    """
    total = float(mix["net_sales"].sum())
    labels = list(mix["channel"])
    values = [float(v) for v in mix["net_sales"]]
    # Compact money above, share below, on each slice.
    texts = [f"<b>{fmt_compact(v)}</b>" for v in values]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.62,
            marker=dict(
                colors=[CHANNEL_COLORS.get(c, Palette.GOLD) for c in labels],
                line=dict(color=Palette.SURFACE, width=3),
            ),
            text=texts,
            texttemplate="%{text}<br>%{percent}",
            textposition="outside",
            textfont=dict(size=12, color=Palette.INK, family="Inter"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f} ֏<br>%{percent} of net sales<extra></extra>",
            sort=False,
            direction="clockwise",
            rotation=0,
        )
    )
    fig.update_layout(
        **PLOT_LAYOUT,
        showlegend=True,
        legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center",
                    font=dict(size=12, color=Palette.INK)),
        height=420,
        uniformtext=dict(minsize=10, mode="hide"),
        annotations=[
            dict(
                text=(f"<b>{fmt_compact(total)}</b><br>"
                      f"<span style='font-size:11px;color:{Palette.TEXT_FAINT}'>NET SALES</span>"),
                x=0.5, y=0.5, font=dict(size=19, color=Palette.INK), showarrow=False,
            )
        ],
    )
    return fig


def trend_chart(df: pd.DataFrame, grain: str, by_channel: bool = False,
                value_col: str = "net_sales", value_label: str = "Net sales",
                color: str = Palette.GOLD) -> Any:
    """A measure over time — stacked by channel on the overview, plain elsewhere.

    `value_col` exists because the Compliments view has nothing to say about net
    sales: a complimentary check is zero-tender by definition, so plotting net
    there draws a flat line at zero. It charts cost instead.
    """
    b = add_period_bucket(df, grain)
    fig = go.Figure()
    if by_channel:
        present = [c for c in REVENUE_CHANNELS if c in set(b["channel"])]
        for ch in present:
            sub = (
                b[b["channel"] == ch]
                .groupby("period", as_index=False)[value_col].sum()
                .sort_values("period")
            )
            fig.add_bar(
                x=sub["period"], y=sub[value_col], name=ch,
                marker_color=CHANNEL_COLORS.get(ch, Palette.GOLD),
                hovertemplate="<b>" + ch + "</b><br>%{x|%d %b %Y}<br>%{y:,.0f} ֏<extra></extra>",
            )
        fig.update_layout(barmode="stack")
    else:
        agg = b.groupby("period", as_index=False)[value_col].sum().sort_values("period")
        fig.add_bar(
            x=agg["period"], y=agg[value_col], name=value_label,
            marker_color=color,
            hovertemplate="%{x|%d %b %Y}<br>%{y:,.0f} ֏<extra></extra>",
        )
        if len(agg) >= 7:
            roll = agg[value_col].rolling(7, min_periods=3).mean()
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
        yaxis=dict(gridcolor="rgba(15,23,42,.08)", zeroline=False, tickformat=",.0f", tickfont=dict(size=11)),
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
        xaxis=dict(gridcolor="rgba(15,23,42,.08)", zeroline=False, tickformat=",.0f"),
        yaxis=dict(showgrid=False, tickfont=dict(size=11)),
    )
    return fig


DELTA_COLS = ("Δ Net", "Δ Net %", "Δ Orders", "Δ Orders %")


def _fmt_signed(v: Any, pct: bool = False) -> str:
    """A signed figure for a comparison cell; an em dash when there is nothing."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{v * 100:+,.1f}%" if pct else f"{v:+,.0f}"
    except (TypeError, ValueError):
        return "—"


def _delta_class(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "flat"
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "flat"
    return "up" if f > 0 else ("down" if f < 0 else "flat")


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
    if "Δ Net" in disp.columns:
        disp["Δ Net"] = disp["Δ Net"].map(lambda v: _fmt_signed(v))
        disp["Δ Net %"] = disp["Δ Net %"].map(lambda v: _fmt_signed(v, pct=True))
        disp["Δ Orders"] = disp["Δ Orders"].map(lambda v: _fmt_signed(v))
        disp["Δ Orders %"] = disp["Δ Orders %"].map(lambda v: _fmt_signed(v, pct=True))
    return disp


def _esc(s: Any) -> str:
    return (
        str(s).replace("&", "&amp;").replace("<", "&lt;")
        .replace(">", "&gt;").replace('"', "&quot;")
    )


def drilldown_html(table: pd.DataFrame) -> str:
    """Render the drill-down as a themed, scrollable HTML table.

    Comparison columns are coloured from the underlying number, not the
    formatted string, so a value that rounds to +0.0% still reads as a gain.
    """
    disp = style_drilldown(table)
    cols = list(disp.columns)
    muted_cols = {"COGS", "COGS %"}

    head = "".join(
        f'<th class="sep">{_esc(c)}</th>' if c == "Δ Net" else f"<th>{_esc(c)}</th>"
        for c in cols
    )

    body = []
    for idx, r in disp.iterrows():
        cells = []
        for c in cols:
            classes = []
            if c in DELTA_COLS:
                classes.append(_delta_class(table.loc[idx, c]))
            elif c in muted_cols:
                classes.append("muted")
            if c == "Δ Net":
                classes.append("sep")
            cls = f' class="{" ".join(classes)}"' if classes else ""
            cells.append(f"<td{cls}>{_esc(r[c])}</td>")
        body.append(f"<tr>{''.join(cells)}</tr>")

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


def render_drilldown(df: pd.DataFrame, grain: str, title: str, period_text: str, key: str,
                     prev_df: Optional[pd.DataFrame] = None,
                     prev_text: str = "") -> None:
    section_title(f"{title} — {grain.lower()} detail")
    table = (drilldown_with_comparison(df, prev_df, grain)
             if prev_df is not None else drilldown_table(df, grain))
    if table.empty:
        st.info("No transactions recorded for this selection.")
        return

    st.markdown(drilldown_html(table), unsafe_allow_html=True)
    unit = {"Daily": "days", "Weekly": "weeks", "Monthly": "months"}.get(grain, "periods")
    compare_note = (
        f" · Δ columns compare each {unit[:-1]} with the matching one in "
        f"<b>{prev_text}</b>, green for growth, red for decline"
        if prev_df is not None and prev_text else ""
    )
    st.markdown(
        f'<div class="note">{len(table)} {unit}{compare_note} · scroll sideways on a narrow screen.</div>',
        unsafe_allow_html=True,
    )

    mix = (
        revenue_only(df).groupby("channel", as_index=False)["net_sales"].sum()
        if "channel" in df.columns else None
    )
    try:
        # The workbook keeps the plain columns; Δ columns are a screen device.
        xlsx = build_excel(drilldown_table(df, grain), compute_metrics(df),
                           title, period_text, mix)
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
                delta_cell = f'<td style="text-align:right;color:{Palette.TEXT_FAINT}">—</td>'
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

    render_drilldown(cur_rev, grain, "All channels", period_text, key="overview",
                     prev_df=prev_rev if compare else None,
                     prev_text=f"{plo:%d %b} – {phi:%d %b %Y}")


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

    render_drilldown(cur, grain, channel, period_text, key=f"ch_{channel}",
                     prev_df=prev if compare else None,
                     prev_text=f"{plo:%d %b} – {phi:%d %b %Y}")


def view_compliments(
    data: pd.DataFrame, lo: dt.date, hi: dt.date,
    plo: dt.date, phi: dt.date, compare: bool, grain: str, period_text: str,
) -> None:
    cur = slice_period(data, lo, hi, channel="Compliments")
    prev = slice_period(data, plo, phi, channel="Compliments")
    m_cur, m_prev = compute_metrics(cur), compute_metrics(prev)

    st.markdown(
        f'<div class="ch-head"><span class="dot" style="background:{Palette.CH_COMPLIMENTS}"></span>'
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
        st.plotly_chart(
            trend_chart(cur, grain, by_channel=False, value_col="cogs",
                        value_label="Cost of compliments", color=Palette.CRIMSON),
            use_container_width=True, config={"displayModeBar": False},
        )

    render_drilldown(cur, grain, "Compliments", period_text, key="compliments",
                     prev_df=prev if compare else None,
                     prev_text=f"{plo:%d %b} – {phi:%d %b %Y}")


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
    venues: List[str] = field(default_factory=list)


def render_sidebar(data: pd.DataFrame) -> Selection:
    today = dt.date.today()
    presets = build_period_presets(today)

    with st.sidebar:
        st.markdown(
            '<div style="font-size:.72rem;letter-spacing:.18em;text-transform:uppercase;'
            'color:#D4AF37;font-weight:700;margin-bottom:12px">Reporting controls</div>',
            unsafe_allow_html=True,
        )

        # Every fresh open lands on the current month. Streamlit keeps widget
        # state across reruns, so this is seeded once per session rather than
        # forced on each pass, which would fight the user's own selection.
        period_names = list(presets.keys())
        if "period_choice" not in st.session_state:
            st.session_state["period_choice"] = "This month"
        preset = st.selectbox("Period", period_names, key="period_choice")
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

        # Only worth showing when the feed actually carries more than one
        # restaurant; a single-venue operator should not see a filter at all.
        all_venues = sorted({v for v in data.get("venue", pd.Series(dtype=str)).unique() if v})
        venues = all_venues
        if len(all_venues) > 1:
            venues = st.multiselect(
                "Venue", all_venues, default=all_venues,
                help="The OLAP preset groups by RestorauntGroup. Narrow this if "
                     "the figures should cover only one restaurant.",
            ) or all_venues

        st.markdown("---")
        compare = st.toggle("Compare with previous period", value=True)
        prev_lo, prev_hi = previous_period(lo, hi, cmp_mode if preset != "Custom range" else "auto")

        if compare:
            st.markdown(
                f'<div class="note">Current&nbsp;&nbsp;<b style="color:{Palette.INK}">'
                f"{lo:%d %b} – {hi:%d %b %Y}</b><br>Previous&nbsp;&nbsp;"
                f'<b style="color:{Palette.TEXT_MUTED}">{prev_lo:%d %b} – {prev_hi:%d %b %Y}</b></div>',
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
                f'<div class="note" style="color:{Palette.CRIMSON};margin-top:6px">'
                f'Last sync error: {_esc(err.split(chr(10))[0][:120])}…</div>',
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
            if st.button("Full rebuild", use_container_width=True,
                         help="Re-pull the venue's entire trading history from iiko"):
                with st.spinner(f"Rebuilding from {_history_start(today, CFG):%b %Y}…"):
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

    return Selection(lo, hi, prev_lo, prev_hi, compare, grain, preset, snap, venues)


# ==============================================================================
#  SECTION 15 — APPLICATION ENTRY
# ==============================================================================

def _cached_sales() -> pd.DataFrame:
    """Sales mirror, memoised for five minutes so tab switches are instant."""
    return load_sales()


if _HAS_STREAMLIT:
    _cached_sales = st.cache_data(ttl=300, show_spinner=False)(_cached_sales)


# Bumped whenever the theme block below changes, so an existing config written
# by an older build is replaced rather than left in place. Without this marker a
# machine that once ran the dark build would keep its dark config for ever.
THEME_STAMP = "nani-theme-v2-light"

STREAMLIT_CONFIG = f"""# Written automatically by app.py. Safe to edit; the theme
# block is rewritten when app.py ships a new one ({THEME_STAMP}).
[theme]
base                = "light"
primaryColor        = "#A8801A"
backgroundColor     = "#F6F7F9"
secondaryBackgroundColor = "#FFFFFF"
textColor           = "#0F172A"

[server]
headless            = true

[browser]
gatherUsageStats    = false
"""


def ensure_streamlit_config() -> bool:
    """Write .streamlit/config.toml when absent or written by an older build.

    Streamlit reads its theme at server start, so a freshly written config only
    takes effect on the next launch; the CSS covers the first run.
    """
    path = APP_DIR / ".streamlit" / "config.toml"
    try:
        if path.exists() and THEME_STAMP in path.read_text(encoding="utf-8"):
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(STREAMLIT_CONFIG, encoding="utf-8")
        log.info("wrote %s — restart Streamlit to apply the light theme", path)
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
            # The first pull covers the venue's whole history — around fifteen
            # requests — so say so rather than leaving a bare spinner.
            span_start = _history_start(dt.date.today(), CFG)
            n_windows = len(list(_chunk_range(span_start, dt.date.today(), CFG.chunk_days)))
            with st.spinner(
                f"First run — loading sales since {span_start:%B %Y} "
                f"({n_windows} requests). This takes a minute; later refreshes are instant."
            ):
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

    # Apply the venue filter once, here, so every tab and export below sees the
    # same universe of rows.
    if sel.venues and "venue" in data.columns:
        present = {v for v in data["venue"].unique() if v}
        if present and set(sel.venues) != present:
            data = data[data["venue"].isin(sel.venues)].copy()

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
            detail = str(res.get("error", "unknown error"))
            # The probe log runs to dozens of lines; a wall of red is unreadable
            # and buries the one sentence that matters.
            headline = detail.split("\n")[0][:240]
            st.error(f"Sync failed ({res['mode']}): {headline}")
            with st.expander("Full diagnostic"):
                st.code(detail)

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

    print("\n  Request headers that will be sent:")
    for key, val in _browser_headers(CFG).items():
        shown = val if len(val) <= 78 else val[:75] + "…"
        print(f"    {key}: {shown}")
    print(f"    X-Correlation-Id: {uuid.uuid4()}   (fresh per request)")
    print("    Content-Type: application/json   (set by requests via json=)")

    sample = _grouped_table_body(dt.date.today() - dt.timedelta(days=7), dt.date.today(), CFG)
    print("\n  Request payload that will be sent:")
    for line in json.dumps(sample, indent=2, ensure_ascii=False).splitlines():
        print(f"    {line}")
    if not CFG.include_non_business:
        print("\n  NOTE  includeNonBusinessPaymentTypes is false, matching the portal's")
        print("        own request. Complimentary '(без оплаты)' checks are very likely")
        print("        excluded by that flag, which would leave the Compliments tab")
        print("        empty. Set IIKO_INCLUDE_NON_BUSINESS=1 to pull them in.")
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
        print(f"  [ok]   payload shape  : {client.body_shape or '(unrecorded)'}")
        print(f"  [ok]   sample pull: {len(df)} rows over the last 7 days")
        if not df.empty:
            print(f"         channels: {sorted(df['channel'].unique())}")
            print(f"         tenders : {sorted(df['payment_type'].unique())[:12]}")
            print(f"         net sales: {df['net_sales'].sum():,.0f} {CURRENCY}")
        else:
            print("         (endpoint answered but returned no rows for this window)")
    except Exception as exc:
        print(f"  [FAIL] sample pull:\n         {exc}")
        if getattr(client, "preset_missing", False):
            print("\n  DIAGNOSIS: the server says the saved report behind this preset")
            print("  id does not exist. That is a 400 'Request data not found', not a")
            print("  routing error — the URL is right, the report it names is gone.")
            print(f"  IIKO_PRESET_ID is currently: {CFG.preset_id}")
            print("  Open the report in iikoWeb, copy the id from the address bar,")
            print("  and set IIKO_PRESET_ID in .env to match.")
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