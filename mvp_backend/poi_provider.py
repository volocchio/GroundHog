"""
POI Provider — Adventure Mode
==============================
Pulls "cool place to land" candidate Points-Of-Interest from OpenStreetMap
via the public Overpass API and caches them in a local SQLite DB keyed by
H3-ish geographic cell so a region is fetched at most once per TTL.

Vibe categories (MVP)
---------------------
- hike          → trailheads, peaks, viewpoints, wilderness areas
- camp          → campsites, primitive camps
- fish          → named lakes / rivers / fishing spots
- ski           → ski resorts / winter-sports areas
- hot_springs   → natural hot springs
- scenic        → scenic viewpoints, mountain peaks
- food          → restaurants, bars/breweries (paired with a landing site)

This is intentionally simple and dependency-light: just `requests` (already
in requirements.txt) + sqlite3 + json. No `h3` lib — we use a plain
0.5° × 0.5° lat/lon grid as the cache cell, which is fine for our region
sizes and avoids another binary dependency.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional

import requests

# ── Configuration ──────────────────────────────────────────────────────

OVERPASS_URL = os.environ.get(
    "GROUNDHOG_OVERPASS_URL",
    "https://overpass-api.de/api/interpreter",
)
USER_AGENT = "GroundHog-AdventureMode/1.0 (+https://groundhog.voloaltro.tech)"
HTTP_TIMEOUT_S = 60
CACHE_TTL_S = 30 * 24 * 3600  # 30 days
CELL_DEG = 0.5  # cache grid cell size

_DB_PATH = os.path.join(os.path.dirname(__file__), "poi_cache.sqlite")
_DB_LOCK = threading.Lock()

# Vibe → list of Overpass query fragments. Each fragment must produce nodes
# with a useful `name` tag (we filter post-hoc to drop unnamed clutter).
_VIBE_QUERIES = {
    "hike": [
        'node["highway"="trailhead"]',
        'node["natural"="peak"]',
        'node["tourism"="viewpoint"]',
        'way["leisure"="nature_reserve"]',
    ],
    "camp": [
        'node["tourism"="camp_site"]',
        'way["tourism"="camp_site"]',
        'node["tourism"="caravan_site"]',
    ],
    "fish": [
        'node["leisure"="fishing"]',
        'way["natural"="water"]["name"]',
        'relation["natural"="water"]["name"]',
    ],
    "ski": [
        'way["landuse"="winter_sports"]',
        'relation["landuse"="winter_sports"]',
        'node["sport"="skiing"]',
    ],
    "hot_springs": [
        'node["natural"="hot_spring"]',
        'node["amenity"="public_bath"]["bath:type"="hot_spring"]',
    ],
    "scenic": [
        'node["tourism"="viewpoint"]',
        'node["natural"="peak"]',
        'node["natural"="waterfall"]',
    ],
    "food": [
        'node["amenity"="restaurant"]',
        'node["amenity"="pub"]',
        'node["craft"="brewery"]',
        'node["amenity"="cafe"]',
    ],
}

VALID_VIBES = tuple(_VIBE_QUERIES.keys())


# ── Data model ─────────────────────────────────────────────────────────

@dataclass
class POI:
    id: str           # "{source}:{osm_id}"
    name: str
    lat: float
    lon: float
    vibe: str         # primary vibe (one of VALID_VIBES)
    osm_type: str     # node/way/relation
    tags: dict        # subset of OSM tags

    def to_dict(self) -> dict:
        return asdict(self)


# ── SQLite cache ───────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _DB_LOCK, _conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS poi (
                id        TEXT PRIMARY KEY,
                name      TEXT NOT NULL,
                lat       REAL NOT NULL,
                lon       REAL NOT NULL,
                vibe      TEXT NOT NULL,
                osm_type  TEXT NOT NULL,
                tags_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_poi_bbox ON poi (lat, lon);
            CREATE INDEX IF NOT EXISTS idx_poi_vibe ON poi (vibe);

            CREATE TABLE IF NOT EXISTS poi_cell (
                cell_key  TEXT NOT NULL,   -- e.g. "47.5_-116.5"
                vibe      TEXT NOT NULL,
                fetched_at INTEGER NOT NULL,
                PRIMARY KEY (cell_key, vibe)
            );
            """
        )


# ── Cell helpers ───────────────────────────────────────────────────────

def _cell_for(lat: float, lon: float) -> tuple[float, float]:
    return (math.floor(lat / CELL_DEG) * CELL_DEG,
            math.floor(lon / CELL_DEG) * CELL_DEG)


def _cell_key(cell_lat: float, cell_lon: float) -> str:
    return f"{cell_lat:.2f}_{cell_lon:.2f}"


def _cells_covering_bbox(south: float, west: float,
                         north: float, east: float) -> List[tuple[float, float]]:
    cells = []
    lat = math.floor(south / CELL_DEG) * CELL_DEG
    while lat < north:
        lon = math.floor(west / CELL_DEG) * CELL_DEG
        while lon < east:
            cells.append((lat, lon))
            lon += CELL_DEG
        lat += CELL_DEG
    return cells


# ── Overpass client ────────────────────────────────────────────────────

def _build_query(vibe: str, south: float, west: float,
                 north: float, east: float) -> str:
    bbox = f"({south:.4f},{west:.4f},{north:.4f},{east:.4f})"
    fragments = _VIBE_QUERIES.get(vibe, [])
    body = "\n".join(f"  {frag}{bbox};" for frag in fragments)
    # `out center` returns a representative point for ways/relations.
    return f"[out:json][timeout:50];\n(\n{body}\n);\nout center tags;"


def _overpass_fetch(query: str) -> list[dict]:
    headers = {"User-Agent": USER_AGENT}
    r = requests.post(OVERPASS_URL, data={"data": query},
                      headers=headers, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    return r.json().get("elements", [])


def _element_to_poi(el: dict, vibe: str) -> Optional[POI]:
    tags = el.get("tags") or {}
    name = tags.get("name") or tags.get("name:en")
    if not name:
        return None
    if el["type"] == "node":
        lat, lon = el.get("lat"), el.get("lon")
    else:
        c = el.get("center") or {}
        lat, lon = c.get("lat"), c.get("lon")
    if lat is None or lon is None:
        return None
    # Keep only the most useful tag keys — full tag blobs balloon the cache.
    keep = {"name", "ele", "tourism", "natural", "leisure", "landuse",
            "amenity", "sport", "highway", "wikipedia", "wikidata",
            "operator", "access", "fee", "website", "phone", "cuisine"}
    slim = {k: v for k, v in tags.items() if k in keep}
    return POI(
        id=f"osm:{el['type']}:{el['id']}",
        name=name,
        lat=float(lat),
        lon=float(lon),
        vibe=vibe,
        osm_type=el["type"],
        tags=slim,
    )


def _store_pois(pois: Iterable[POI]) -> int:
    n = 0
    with _DB_LOCK, _conn() as c:
        for p in pois:
            c.execute(
                "INSERT OR REPLACE INTO poi (id, name, lat, lon, vibe, osm_type, tags_json) "
                "VALUES (?,?,?,?,?,?,?)",
                (p.id, p.name, p.lat, p.lon, p.vibe, p.osm_type, json.dumps(p.tags)),
            )
            n += 1
    return n


def _mark_cell(cell_lat: float, cell_lon: float, vibe: str) -> None:
    with _DB_LOCK, _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO poi_cell (cell_key, vibe, fetched_at) VALUES (?,?,?)",
            (_cell_key(cell_lat, cell_lon), vibe, int(time.time())),
        )


def _cell_is_fresh(cell_lat: float, cell_lon: float, vibe: str) -> bool:
    with _DB_LOCK, _conn() as c:
        row = c.execute(
            "SELECT fetched_at FROM poi_cell WHERE cell_key=? AND vibe=?",
            (_cell_key(cell_lat, cell_lon), vibe),
        ).fetchone()
    if not row:
        return False
    return (time.time() - row["fetched_at"]) < CACHE_TTL_S


# ── Public API ─────────────────────────────────────────────────────────

def ensure_region(south: float, west: float, north: float, east: float,
                  vibes: Iterable[str]) -> dict:
    """Make sure cache covers (bbox × vibes). Returns stats dict."""
    init_db()
    fetched = 0
    skipped = 0
    errors: list[str] = []
    for vibe in vibes:
        if vibe not in _VIBE_QUERIES:
            continue
        for (cell_lat, cell_lon) in _cells_covering_bbox(south, west, north, east):
            if _cell_is_fresh(cell_lat, cell_lon, vibe):
                skipped += 1
                continue
            q = _build_query(vibe,
                             cell_lat, cell_lon,
                             cell_lat + CELL_DEG, cell_lon + CELL_DEG)
            try:
                els = _overpass_fetch(q)
            except Exception as e:  # noqa: BLE001
                errors.append(f"{vibe}@{_cell_key(cell_lat, cell_lon)}: {e}")
                # Don't mark cell fresh — try again next call.
                continue
            pois = [p for p in (_element_to_poi(el, vibe) for el in els) if p]
            _store_pois(pois)
            _mark_cell(cell_lat, cell_lon, vibe)
            fetched += len(pois)
    return {"fetched": fetched, "cells_skipped": skipped, "errors": errors}


def query(south: float, west: float, north: float, east: float,
          vibes: Iterable[str], limit: int = 500) -> List[POI]:
    """Read POIs from the local cache (does NOT trigger Overpass)."""
    init_db()
    vibes = list(vibes)
    if not vibes:
        return []
    placeholders = ",".join("?" for _ in vibes)
    sql = (
        f"SELECT * FROM poi "
        f"WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? "
        f"AND vibe IN ({placeholders}) "
        f"LIMIT ?"
    )
    args = [south, north, west, east, *vibes, limit]
    with _DB_LOCK, _conn() as c:
        rows = c.execute(sql, args).fetchall()
    out: List[POI] = []
    for r in rows:
        out.append(POI(
            id=r["id"], name=r["name"], lat=r["lat"], lon=r["lon"],
            vibe=r["vibe"], osm_type=r["osm_type"],
            tags=json.loads(r["tags_json"]),
        ))
    return out


def stats() -> dict:
    init_db()
    with _DB_LOCK, _conn() as c:
        total = c.execute("SELECT COUNT(*) AS n FROM poi").fetchone()["n"]
        by_vibe = {r["vibe"]: r["n"] for r in c.execute(
            "SELECT vibe, COUNT(*) AS n FROM poi GROUP BY vibe").fetchall()}
        cells = c.execute("SELECT COUNT(*) AS n FROM poi_cell").fetchone()["n"]
    return {"total_pois": total, "by_vibe": by_vibe, "cells_cached": cells}
