"""
Landing Areas — bbox-scoped OSM polygons that look landable for a heli.

Returns the same "good" surface tags the Adventure-mode surveyor uses
(farmland / meadow / grass / grassland / golf / park / aerodrome /
airstrip / helipad), filtered by terrain slope sampled at the polygon
centroid (SRTM). Cached in landing_areas.sqlite keyed by rounded bbox
tile + slope threshold so repeat panning is free.

Designed for the map-overlay layer ("Safe Landings (OSM)"). Capped at
a moderate bbox size so a Overpass call stays under a few seconds and
we don't accidentally hammer the public mirror.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from typing import Optional

import requests

from mvp_backend.poi_provider import OVERPASS_URL, USER_AGENT
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.landing_sites import estimate_slope_deg

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRTM_DIR = os.path.join(ROOT, "mvp_backend", "srtm_cache")
_DB_PATH = os.path.join(os.path.dirname(__file__), "landing_areas.sqlite")

CACHE_TTL_S = 30 * 24 * 3600   # 30 days
HTTP_TIMEOUT_S = 25
# Hard cap so a single call cannot blow up Overpass / our own slope
# sampling. ~0.5 sq deg ≈ a comfortable mid-zoom map view.
MAX_BBOX_SQ_DEG = 0.5
# Clip very large polygons (huge meadows / parks) to keep payload sane.
MAX_RING_POINTS = 200
# Limit per-call return size.
MAX_FEATURES = 600
# Tile rounding for the cache key.
_TILE_DEG = 0.1

_DB_LOCK = threading.Lock()


# ── Schema ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _DB_LOCK, _conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS landing_areas (
                key         TEXT PRIMARY KEY,
                features_json TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL
            );
            """
        )


def _cache_key(s: float, n: float, w: float, e: float, slope_deg: float) -> str:
    def r(v: float) -> str:
        return f"{math.floor(v / _TILE_DEG) * _TILE_DEG:.1f}"
    return f"{r(s)}_{r(n)}_{r(w)}_{r(e)}_{slope_deg:.1f}"


# ── Overpass ───────────────────────────────────────────────────────────

# (overpass fragment, label, kind)
# Only "good" surfaces — caution / avoid would be misleading on a
# pre-flight landing map.
_GOOD_QUERIES = [
    ('node["aeroway"~"aerodrome|airstrip|airfield|helipad"]',  'Helipad / airstrip',  'airstrip'),
    ('way["aeroway"~"aerodrome|airstrip|airfield|runway|helipad"]', 'Helipad / airstrip', 'airstrip'),
    ('way["landuse"="farmland"]',     'Farmland',  'farmland'),
    ('way["landuse"="meadow"]',       'Meadow',    'meadow'),
    ('way["landuse"="grass"]',        'Grass',     'grass'),
    ('way["natural"="grassland"]',    'Grassland', 'grassland'),
    ('way["leisure"="golf_course"]',  'Golf course', 'golf'),
    ('way["leisure"="park"]',         'Park',      'park'),
]


def _label_for_tags(tags: dict) -> tuple[str, str]:
    if not tags:
        return ('Open ground', 'unknown')
    if tags.get('aeroway') in ('aerodrome', 'airstrip', 'airfield', 'runway', 'helipad'):
        return ('Helipad / airstrip', 'airstrip')
    if tags.get('landuse') == 'farmland':
        return ('Farmland', 'farmland')
    if tags.get('landuse') == 'meadow':
        return ('Meadow', 'meadow')
    if tags.get('landuse') == 'grass':
        return ('Grass', 'grass')
    if tags.get('natural') == 'grassland':
        return ('Grassland', 'grassland')
    if tags.get('leisure') == 'golf_course':
        return ('Golf course', 'golf')
    if tags.get('leisure') == 'park':
        return ('Park', 'park')
    return ('Open ground', 'unknown')


def _overpass_bbox(south: float, north: float,
                   west: float, east: float) -> list[dict]:
    bbox = f"{south:.6f},{west:.6f},{north:.6f},{east:.6f}"
    parts = []
    for frag, _, _ in _GOOD_QUERIES:
        if frag.startswith('node'):
            base, rest = 'node', frag[4:]
        elif frag.startswith('way'):
            base, rest = 'way', frag[3:]
        else:
            continue
        parts.append(f'{base}{rest}({bbox});')
    q = (
        '[out:json][timeout:25];'
        '(' + ''.join(parts) + ');'
        f'out tags geom {MAX_FEATURES};'
    )
    r = requests.post(
        OVERPASS_URL,
        data={'data': q},
        headers={'User-Agent': USER_AGENT},
        timeout=HTTP_TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json().get('elements', [])


# ── Slope-aware filtering ──────────────────────────────────────────────

def _ring_centroid(ring: list[tuple[float, float]]) -> tuple[float, float]:
    if not ring:
        return (0.0, 0.0)
    lat = sum(p[0] for p in ring) / len(ring)
    lon = sum(p[1] for p in ring) / len(ring)
    return (lat, lon)


def _ring_area_sq_nm(ring: list[tuple[float, float]]) -> float:
    """Approximate planar area in nm² (good enough for ranking / filtering)."""
    if len(ring) < 3:
        return 0.0
    lat0 = ring[0][0]
    cos_lat = math.cos(math.radians(lat0))
    pts = [((p[1] - ring[0][1]) * 60.0 * cos_lat,
            (p[0] - ring[0][0]) * 60.0) for p in ring]
    a = 0.0
    for i in range(len(pts)):
        x1, y1 = pts[i]
        x2, y2 = pts[(i + 1) % len(pts)]
        a += x1 * y2 - x2 * y1
    return abs(a) * 0.5


# ── Public API ─────────────────────────────────────────────────────────

def query_bbox(south: float, north: float, west: float, east: float,
               max_slope_deg: float = 8.0,
               use_cache: bool = True) -> dict:
    """Return landable OSM polygons inside the bbox passing the slope filter.

    Result schema:
      {
        "features": [
          {"kind": "...", "label": "...", "lat": .., "lon": ..,
           "slope_deg": ..., "elev_ft": ..., "area_sq_nm": ...,
           "ring": [[lat,lon],...]}, ...
        ],
        "cached": bool, "error": str|None,
        "max_slope_deg": float, "bbox_too_large": bool,
      }
    """
    init_db()
    sq_deg = max(0.0, north - south) * max(0.0, east - west)
    if sq_deg <= 0 or sq_deg > MAX_BBOX_SQ_DEG:
        return {
            'features': [], 'cached': False, 'error': None,
            'max_slope_deg': max_slope_deg, 'bbox_too_large': True,
        }

    k = _cache_key(south, north, west, east, max_slope_deg)
    if use_cache:
        with _DB_LOCK, _conn() as c:
            row = c.execute(
                "SELECT * FROM landing_areas WHERE key=?", (k,)
            ).fetchone()
        if row and (time.time() - row['fetched_at']) < CACHE_TTL_S:
            try:
                feats = json.loads(row['features_json'])
            except json.JSONDecodeError:
                feats = []
            return {
                'features': feats, 'cached': True, 'error': None,
                'max_slope_deg': max_slope_deg, 'bbox_too_large': False,
            }

    try:
        elements = _overpass_bbox(south, north, west, east)
    except Exception as e:  # noqa: BLE001
        return {
            'features': [], 'cached': False,
            'error': f'{type(e).__name__}: {e}',
            'max_slope_deg': max_slope_deg, 'bbox_too_large': False,
        }

    provider = SRTMProvider(cache_dir=_SRTM_DIR)
    out: list[dict] = []
    for el in elements:
        tags = el.get('tags') or {}
        label, kind = _label_for_tags(tags)
        # Build ring (lat, lon) for ways; for nodes just use a tiny ring.
        ring: list[tuple[float, float]] = []
        if el.get('type') == 'way':
            geom = el.get('geometry') or []
            ring = [(g['lat'], g['lon']) for g in geom if 'lat' in g and 'lon' in g]
            if len(ring) >= 2 and ring[0] != ring[-1]:
                ring.append(ring[0])
        elif el.get('type') == 'node':
            lat = el.get('lat'); lon = el.get('lon')
            if lat is None or lon is None:
                continue
            d = 0.0003   # ~30 m square so it's clickable
            ring = [(lat - d, lon - d), (lat - d, lon + d),
                    (lat + d, lon + d), (lat + d, lon - d), (lat - d, lon - d)]
        if len(ring) < 3:
            continue

        clat, clon = _ring_centroid(ring)
        slope = estimate_slope_deg(clat, clon, provider=provider)
        if math.isnan(slope) or slope > max_slope_deg:
            continue

        elev_m = provider.get_many_m([(clat, clon)])[0]
        elev_ft = float(meters_to_feet(elev_m)) if elev_m == elev_m else 0.0

        # Optionally simplify big rings.
        if len(ring) > MAX_RING_POINTS:
            step = max(1, len(ring) // MAX_RING_POINTS)
            ring = ring[::step] + [ring[-1]]

        area_nm2 = _ring_area_sq_nm(ring)

        out.append({
            'kind': kind,
            'label': label,
            'lat': round(clat, 6),
            'lon': round(clon, 6),
            'slope_deg': round(slope, 1),
            'elev_ft': round(elev_ft, 0),
            'area_sq_nm': round(area_nm2, 4),
            'ring': [[round(p[0], 6), round(p[1], 6)] for p in ring],
        })
        if len(out) >= MAX_FEATURES:
            break

    # Cache result.
    try:
        with _DB_LOCK, _conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO landing_areas(key, features_json, fetched_at) "
                "VALUES(?,?,?)",
                (k, json.dumps(out), int(time.time())),
            )
            c.commit()
    except sqlite3.Error:
        pass

    return {
        'features': out, 'cached': False, 'error': None,
        'max_slope_deg': max_slope_deg, 'bbox_too_large': False,
    }
