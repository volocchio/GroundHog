"""
Landcover — bbox-scoped OSM polygons that drive cost-aware routing
and a map overlay layer.

Distinct from landing_areas (which surfaces *only* good landable
surfaces for the user-visible "Safe Landings" overlay): this module
returns a wider mix of features the *planner* needs to know about:

  - HIGHWAYS (roads): primary/secondary/trunk/motorway. Long, narrow,
    often hard-surfaced corridors that historically have been used as
    emergency landing areas — and even when not landable, they at
    least mean an injured pilot can walk to help. Treated as a mild
    BONUS in the cost grid.

  - FOREST / WOOD: dense canopy is a known killer for autorotation
    flares (rotor strike on trees, no run-on). Mild PENALTY.

  - URBAN (residential/industrial): buildings, wires, no run-on.
    Stronger PENALTY than forest.

  - PARK / FARMLAND / MEADOW: NEUTRAL — already covered by the
    landing-areas surveyor.

Cached in landcover.sqlite keyed by rounded bbox tile so repeat panning
and repeat planner calls in the same area are free.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time

import requests

from mvp_backend.poi_provider import OVERPASS_URL, USER_AGENT

_DB_PATH = os.path.join(os.path.dirname(__file__), "landcover.sqlite")

CACHE_TTL_S = 30 * 24 * 3600   # 30 days
HTTP_TIMEOUT_S = 30
# Larger than landing-areas — the planner asks for the full leg bbox,
# which on a 200 nm leg can easily exceed 1 sq deg. Cap at something
# the public Overpass mirrors will still return without timing out.
MAX_BBOX_SQ_DEG = 2.0
MAX_RING_POINTS = 120
MAX_FEATURES = 4000
_TILE_DEG = 0.1

_DB_LOCK = threading.Lock()


# ── Categorisation ─────────────────────────────────────────────────────

# (kind, overpass_fragment, base_cost_delta)
# Cost convention: positive = penalty, negative = bonus. Combined and
# clamped per-cell in planner._build_landcover_cost.
_QUERIES = [
    ('road',     'way["highway"~"motorway|trunk|primary|secondary"]', -0.30),
    ('forest',   'way["natural"="wood"]',                               +0.30),
    ('forest',   'way["landuse"="forest"]',                             +0.30),
    ('urban',    'way["landuse"~"residential|industrial|commercial"]',  +0.80),
    ('urban',    'way["landuse"="military"]',                           +1.20),
    ('open',     'way["landuse"~"farmland|meadow|grass"]',              -0.05),
    ('open',     'way["natural"="grassland"]',                          -0.05),
]


def _kind_and_cost(tags: dict) -> tuple[str, float]:
    if not tags:
        return ('unknown', 0.0)
    hw = tags.get('highway')
    if hw in ('motorway', 'trunk', 'primary', 'secondary'):
        return ('road', -0.30)
    if tags.get('natural') == 'wood' or tags.get('landuse') == 'forest':
        return ('forest', +0.30)
    if tags.get('landuse') in ('residential', 'industrial', 'commercial'):
        return ('urban', +0.80)
    if tags.get('landuse') == 'military':
        return ('urban', +1.20)
    if (tags.get('landuse') in ('farmland', 'meadow', 'grass')
            or tags.get('natural') == 'grassland'):
        return ('open', -0.05)
    return ('unknown', 0.0)


# ── Schema ─────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _DB_LOCK, _conn() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS landcover (
                key         TEXT PRIMARY KEY,
                features_json TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL
            );
            """
        )


def _cache_key(s: float, n: float, w: float, e: float) -> str:
    def r(v: float) -> str:
        return f"{math.floor(v / _TILE_DEG) * _TILE_DEG:.1f}"
    return f"{r(s)}_{r(n)}_{r(w)}_{r(e)}"


# ── Overpass ───────────────────────────────────────────────────────────

def _overpass_bbox(south: float, north: float,
                   west: float, east: float) -> list[dict]:
    bbox = f"{south:.6f},{west:.6f},{north:.6f},{east:.6f}"
    parts = []
    for _, frag, _ in _QUERIES:
        # All current fragments are way[...]; keep generic just in case.
        if frag.startswith('way'):
            base, rest = 'way', frag[3:]
        elif frag.startswith('node'):
            base, rest = 'node', frag[4:]
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


# ── Public API ─────────────────────────────────────────────────────────

def query_bbox(south: float, north: float, west: float, east: float,
               use_cache: bool = True) -> dict:
    """Return categorised OSM landcover features inside the bbox.

    Schema:
      {
        "features": [
          {"kind": "road|forest|urban|open|unknown",
           "cost_delta": float,
           "ring": [[lat,lon],...]},
          ...
        ],
        "cached": bool, "error": str|None,
        "bbox_too_large": bool,
      }

    Highways (linear ways) are returned with the raw way geometry as
    `ring` (open, not closed) — the planner rasterises them as
    line-strings.
    """
    init_db()
    sq_deg = max(0.0, north - south) * max(0.0, east - west)
    if sq_deg <= 0 or sq_deg > MAX_BBOX_SQ_DEG:
        return {
            'features': [], 'cached': False, 'error': None,
            'bbox_too_large': True,
        }

    k = _cache_key(south, north, west, east)
    if use_cache:
        with _DB_LOCK, _conn() as c:
            row = c.execute(
                "SELECT * FROM landcover WHERE key=?", (k,)
            ).fetchone()
        if row and (time.time() - row['fetched_at']) < CACHE_TTL_S:
            try:
                feats = json.loads(row['features_json'])
            except json.JSONDecodeError:
                feats = []
            return {
                'features': feats, 'cached': True, 'error': None,
                'bbox_too_large': False,
            }

    try:
        elements = _overpass_bbox(south, north, west, east)
    except Exception as e:  # noqa: BLE001
        return {
            'features': [], 'cached': False,
            'error': f'{type(e).__name__}: {e}',
            'bbox_too_large': False,
        }

    out: list[dict] = []
    for el in elements:
        tags = el.get('tags') or {}
        kind, cost = _kind_and_cost(tags)
        if kind == 'unknown':
            continue
        geom = el.get('geometry') or []
        ring = [(g['lat'], g['lon']) for g in geom if 'lat' in g and 'lon' in g]
        if len(ring) < 2:
            continue
        # Polygons (forest, urban, open) close the ring; roads stay open.
        if kind != 'road':
            if len(ring) < 3:
                continue
            if ring[0] != ring[-1]:
                ring.append(ring[0])
        if len(ring) > MAX_RING_POINTS:
            step = max(1, len(ring) // MAX_RING_POINTS)
            ring = ring[::step] + [ring[-1]]
        out.append({
            'kind': kind,
            'cost_delta': cost,
            'ring': [[round(p[0], 6), round(p[1], 6)] for p in ring],
        })
        if len(out) >= MAX_FEATURES:
            break

    try:
        with _DB_LOCK, _conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO landcover(key, features_json, fetched_at) "
                "VALUES(?,?,?)",
                (k, json.dumps(out), int(time.time())),
            )
            c.commit()
    except sqlite3.Error:
        pass

    return {
        'features': out, 'cached': False, 'error': None,
        'bbox_too_large': False,
    }
