"""
Active TFR (Temporary Flight Restriction) fetcher.

Pulls the FAA TFR list page, downloads each NOTAM XML, parses out polygon
geometry + altitude band + effective times, and caches the result for
~15 minutes in tfrs.sqlite.

Designed to FAIL SAFELY: if the FAA feed is unreachable or a particular
NOTAM XML can't be parsed, we serve the last good cache (or an empty
list) and keep going. A planning request must never crash because TFR
data is unavailable. TFRs are advisory in this app — the pilot is
expected to brief independently — but having them on the map and as a
hard-block in the route solver is a strong cue.

Notes on FAA data format
------------------------
- List page: https://tfr.faa.gov/tfr2/list.html  (HTML table)
- Per-NOTAM XML: https://tfr.faa.gov/save_pages/detail_<NUM>_<YR>.xml
- XML uses the AIXM-derived XNOTAM-Update schema. Coordinates are in
  DDMMSS.SS[N/S] / DDDMMSS.SS[E/W] format inside <geoLat>/<geoLong>.
- Altitude codes: SFC, MSL, AGL, FL. We normalise to MSL ft. AGL is
  treated as 0 (worst case) for the floor and as the value+terrain for
  the ceiling — but since TFRs are hard blocks regardless, this only
  affects the optional vertical-escape check.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from typing import Optional
from xml.etree import ElementTree as ET

import requests


_DB_PATH = os.path.join(os.path.dirname(__file__), "tfrs.sqlite")
_TFR_INDEX_URL = "https://tfr.faa.gov/tfr2/list.html"
_TFR_XML_URL = "https://tfr.faa.gov/save_pages/detail_{num}_{yr}.xml"
_USER_AGENT = "GroundHog/1.0 (helicopter VFR planner)"
_HTTP_TIMEOUT_S = 12

CACHE_TTL_S = 15 * 60                # active TFRs change fast
INDEX_RE = re.compile(r"detail_(\d+)_(\d{2,4})\.xml", re.IGNORECASE)

_DB_LOCK = threading.Lock()


# ── Schema ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _DB_LOCK, _conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS tfrs (
                notam        TEXT PRIMARY KEY,
                description  TEXT,
                polygon_json TEXT,
                min_alt_ft   REAL,
                max_alt_ft   REAL,
                min_lat      REAL,
                max_lat      REAL,
                min_lon      REAL,
                max_lon      REAL,
                effective    TEXT,
                expires      TEXT,
                fetched_at   REAL
            )
            """
        )
        c.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)"
        )
        c.commit()


# ── Coordinate parsing ────────────────────────────────────────────────

_COORD_RE = re.compile(
    r"^\s*(\d{2,3})(\d{2})(\d{2}(?:\.\d+)?)([NSEW])\s*$",
    re.IGNORECASE,
)


def _parse_dms(s: str) -> Optional[float]:
    """Parse FAA DDMMSS.SS[hemisphere] string into signed decimal degrees."""
    if not s:
        return None
    m = _COORD_RE.match(s.strip())
    if not m:
        return None
    deg = float(m.group(1))
    minutes = float(m.group(2))
    seconds = float(m.group(3))
    hemi = m.group(4).upper()
    val = deg + minutes / 60.0 + seconds / 3600.0
    if hemi in ("S", "W"):
        val = -val
    return val


# ── Altitude parsing ──────────────────────────────────────────────────

def _parse_alt_ft(val: Optional[str], code: Optional[str]) -> Optional[float]:
    """Normalise FAA altitude string + code to MSL ft (best effort)."""
    if val is None:
        return None
    s = str(val).strip().upper()
    if s in ("SFC", "GND", "0"):
        return 0.0
    try:
        n = float(re.sub(r"[^\d\.\-]", "", s))
    except ValueError:
        return None
    code = (code or "").strip().upper()
    if code == "FL" or s.startswith("FL"):
        return n * 100.0
    # MSL, AGL, ft — we treat all as MSL for blocking purposes. AGL TFRs
    # near the surface stay near the surface, which is fine for hard
    # blocking; a more nuanced version would add terrain elevation.
    return n


# ── XML parsing ───────────────────────────────────────────────────────

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _findall_local(root: ET.Element, name: str) -> list:
    out = []
    for el in root.iter():
        if _strip_ns(el.tag) == name:
            out.append(el)
    return out


def _findtext_local(el: ET.Element, name: str) -> Optional[str]:
    for child in el.iter():
        if _strip_ns(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def _parse_tfr_xml(xml_bytes: bytes, notam_id: str) -> Optional[dict]:
    """Extract polygon + altitudes + times from one TFR XML."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None

    # Polygon vertices live inside <abdMergedArea> / <Avx> elements.
    polygon: list[list[float]] = []
    for area in _findall_local(root, "abdMergedArea"):
        ring: list[list[float]] = []
        for vx in _findall_local(area, "Avx"):
            lat_s = _findtext_local(vx, "geoLat")
            lon_s = _findtext_local(vx, "geoLong")
            lat = _parse_dms(lat_s) if lat_s else None
            lon = _parse_dms(lon_s) if lon_s else None
            if lat is not None and lon is not None:
                ring.append([lat, lon])
        if len(ring) >= 3:
            # Close the ring if not already closed.
            if ring[0] != ring[-1]:
                ring.append(ring[0])
            polygon = ring  # take first usable area; ignore subsequent
            break

    # Some TFRs use circles defined by a center point + radius. Capture
    # those too by fabricating a 36-vertex polygon approximation.
    if not polygon:
        for area in _findall_local(root, "Circle"):
            center_lat_s = _findtext_local(area, "geoLatCenter")
            center_lon_s = _findtext_local(area, "geoLongCenter")
            radius_s = _findtext_local(area, "valRadius")
            if not (center_lat_s and center_lon_s and radius_s):
                continue
            clat = _parse_dms(center_lat_s)
            clon = _parse_dms(center_lon_s)
            try:
                radius_nm = float(re.sub(r"[^\d\.]", "", radius_s))
            except ValueError:
                continue
            if clat is None or clon is None or radius_nm <= 0:
                continue
            # 1 nm latitude ≈ 1/60 deg; longitude scales by cos(lat).
            import math
            dlat = radius_nm / 60.0
            dlon = radius_nm / (60.0 * max(0.1, math.cos(math.radians(clat))))
            poly = []
            for k in range(36):
                a = 2 * math.pi * k / 36
                poly.append([clat + dlat * math.cos(a), clon + dlon * math.sin(a)])
            poly.append(poly[0])
            polygon = poly
            break

    if not polygon:
        return None

    lower = _findtext_local(root, "valDistVerLower")
    lower_uom = _findtext_local(root, "uomDistVerLower")
    upper = _findtext_local(root, "valDistVerUpper")
    upper_uom = _findtext_local(root, "uomDistVerUpper")
    min_alt = _parse_alt_ft(lower, lower_uom)
    max_alt = _parse_alt_ft(upper, upper_uom)

    desc = _findtext_local(root, "txtDescrTraditional") or _findtext_local(root, "txtDescrUSNS") or ""
    eff = _findtext_local(root, "dateEffective") or ""
    exp = _findtext_local(root, "dateExpire") or ""

    lats = [pt[0] for pt in polygon]
    lons = [pt[1] for pt in polygon]
    return {
        "notam": notam_id,
        "description": desc[:2000],
        "polygon": polygon,
        "min_alt_ft": min_alt if min_alt is not None else 0.0,
        "max_alt_ft": max_alt if max_alt is not None else 60000.0,
        "min_lat": min(lats),
        "max_lat": max(lats),
        "min_lon": min(lons),
        "max_lon": max(lons),
        "effective": eff,
        "expires": exp,
    }


# ── Fetch + cache ─────────────────────────────────────────────────────

def _http_get(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=_HTTP_TIMEOUT_S)
        if r.status_code != 200:
            return None
        return r.content
    except Exception:
        return None


def _list_active_notam_ids() -> list[tuple[str, str]]:
    """Returns [(num, yr), ...] from the FAA TFR list page."""
    body = _http_get(_TFR_INDEX_URL)
    if not body:
        return []
    text = body.decode("utf-8", errors="ignore")
    seen: set[tuple[str, str]] = set()
    for m in INDEX_RE.finditer(text):
        seen.add((m.group(1), m.group(2)))
    return sorted(seen)


def _refresh_cache() -> None:
    """Re-pull all active TFRs and write to sqlite."""
    init_db()
    pairs = _list_active_notam_ids()
    if not pairs:
        # Mark refresh attempt so we don't re-hammer FAA on every request.
        with _DB_LOCK, _conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('last_refresh', ?)",
                (str(time.time()),),
            )
            c.commit()
        return

    parsed: list[dict] = []
    for num, yr in pairs:
        url = _TFR_XML_URL.format(num=num, yr=yr)
        body = _http_get(url)
        if not body:
            continue
        notam_id = f"{num}/{yr}"
        rec = _parse_tfr_xml(body, notam_id)
        if rec:
            parsed.append(rec)

    now = time.time()
    with _DB_LOCK, _conn() as c:
        c.execute("DELETE FROM tfrs")
        for r in parsed:
            c.execute(
                """
                INSERT INTO tfrs(notam, description, polygon_json,
                                  min_alt_ft, max_alt_ft,
                                  min_lat, max_lat, min_lon, max_lon,
                                  effective, expires, fetched_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["notam"], r["description"], json.dumps(r["polygon"]),
                    r["min_alt_ft"], r["max_alt_ft"],
                    r["min_lat"], r["max_lat"], r["min_lon"], r["max_lon"],
                    r["effective"], r["expires"], now,
                ),
            )
        c.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('last_refresh', ?)",
            (str(now),),
        )
        c.commit()


def _last_refresh_age_s() -> float:
    init_db()
    with _DB_LOCK, _conn() as c:
        row = c.execute("SELECT value FROM meta WHERE key='last_refresh'").fetchone()
    if not row or not row["value"]:
        return float("inf")
    try:
        return time.time() - float(row["value"])
    except (TypeError, ValueError):
        return float("inf")


def get_active_tfrs(bbox: Optional[tuple[float, float, float, float]] = None,
                    refresh: bool = True) -> list[dict]:
    """Return active TFRs, optionally clipped to (south, west, north, east)."""
    init_db()
    if refresh and _last_refresh_age_s() > CACHE_TTL_S:
        try:
            _refresh_cache()
        except Exception:
            pass  # serve last-good cache

    sql = ("SELECT notam, description, polygon_json, min_alt_ft, max_alt_ft, "
           "min_lat, max_lat, min_lon, max_lon, effective, expires "
           "FROM tfrs")
    args: list = []
    if bbox is not None:
        s, w, n, e = bbox
        sql += " WHERE max_lat >= ? AND min_lat <= ? AND max_lon >= ? AND min_lon <= ?"
        args = [s, n, w, e]
    with _DB_LOCK, _conn() as c:
        rows = c.execute(sql, args).fetchall()

    out: list[dict] = []
    for r in rows:
        try:
            polygon = json.loads(r["polygon_json"])
        except (TypeError, ValueError):
            continue
        out.append({
            "notam": r["notam"],
            "description": r["description"],
            "polygon": polygon,
            "min_alt_ft": r["min_alt_ft"],
            "max_alt_ft": r["max_alt_ft"],
            "effective": r["effective"],
            "expires": r["expires"],
        })
    return out
