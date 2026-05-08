"""
Landing Surveyor — Adventure Mode
==================================
Per-POI Overpass query that asks "what's actually on the ground around
this point?" and returns a categorical verdict for off-airport landing
suitability.

Modeled on the Turnback Simulator's rated-surface scheme (good / caution
/ avoid). Cached in poi_cache.sqlite keyed by rounded coordinate so
repeat searches are free.

Categories (Turnback-style):
  good    — open, landable surfaces: farmland, meadow, grass, golf,
            existing aerodromes/airstrips
  caution — flat-ish but hazardous: major roads (wires, traffic),
            parking lots (light poles, fences)
  avoid   — water, dense forest, built-up
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

from mvp_backend.poi_provider import OVERPASS_URL, USER_AGENT, _DB_PATH

CACHE_TTL_S = 30 * 24 * 3600       # 30 days
HTTP_TIMEOUT_S = 25
DEFAULT_RADIUS_M = 200.0           # heli LZ scout ring
HAZARD_RADIUS_M = 100.0            # tighter ring for "avoid" features

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
            CREATE TABLE IF NOT EXISTS landing_survey (
                key         TEXT PRIMARY KEY,   -- "lat4_lon4_rad"
                lat         REAL NOT NULL,
                lon         REAL NOT NULL,
                radius_m    REAL NOT NULL,
                verdict     TEXT NOT NULL,
                good_count  INTEGER NOT NULL,
                caution_count INTEGER NOT NULL,
                avoid_count INTEGER NOT NULL,
                features_json TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL
            );
            """
        )


def _key(lat: float, lon: float, radius_m: float) -> str:
    # ~11 m precision at 4 decimals — fine for cache reuse on the same POI.
    return f"{lat:.4f}_{lon:.4f}_{int(radius_m)}"


# ── Overpass ───────────────────────────────────────────────────────────

# (overpass fragment, label, rating)
_QUERIES = [
    ('node["aeroway"~"aerodrome|airstrip|airfield|helipad"]', 'Airstrip / helipad', 'good'),
    ('way["aeroway"~"aerodrome|airstrip|airfield|runway|helipad"]', 'Airstrip / helipad', 'good'),
    ('way["landuse"="farmland"]',           'Farmland',     'good'),
    ('way["landuse"="meadow"]',             'Meadow',       'good'),
    ('way["landuse"="grass"]',              'Grass',        'good'),
    ('way["natural"="grassland"]',          'Grassland',    'good'),
    ('way["leisure"="golf_course"]',        'Golf course',  'good'),
    ('way["leisure"="park"]',               'Park',         'good'),
    # Caution: flat but with hazards
    ('way["highway"~"motorway|trunk|primary"]', 'Major road (wires/traffic)', 'caution'),
    ('way["amenity"="parking"]',            'Parking lot (light poles)', 'caution'),
    # Avoid: unlandable
    ('way["natural"="water"]',              'Water',        'avoid'),
    ('way["waterway"~"river|canal"]',       'River / canal', 'avoid'),
    ('way["natural"="wood"]',               'Forest',       'avoid'),
    ('way["landuse"="forest"]',             'Forest',       'avoid'),
    ('way["natural"="wetland"]',            'Wetland',      'avoid'),
    ('way["natural"="scree"]',              'Scree',        'avoid'),
    ('way["natural"="bare_rock"]',          'Rock',         'avoid'),
    ('way["landuse"~"residential|industrial|commercial|retail"]', 'Built-up', 'avoid'),
    # Linear hazards — power lines
    ('way["power"="line"]',                 'Power line',   'avoid'),
]


def _classify_tags(tags: dict) -> tuple[str, str]:
    """Return (label, rating) for an element's tags."""
    if not tags:
        return ('Unknown', 'caution')
    if tags.get('aeroway') in ('aerodrome', 'airstrip', 'airfield', 'runway', 'helipad'):
        return ('Airstrip / helipad', 'good')
    if tags.get('landuse') == 'farmland':
        return ('Farmland', 'good')
    if tags.get('landuse') == 'meadow':
        return ('Meadow', 'good')
    if tags.get('landuse') == 'grass':
        return ('Grass', 'good')
    if tags.get('natural') == 'grassland':
        return ('Grassland', 'good')
    if tags.get('leisure') == 'golf_course':
        return ('Golf course', 'good')
    if tags.get('leisure') == 'park':
        return ('Park', 'good')
    if tags.get('highway') in ('motorway', 'trunk', 'primary'):
        return ('Major road', 'caution')
    if tags.get('amenity') == 'parking':
        return ('Parking lot', 'caution')
    if tags.get('natural') == 'water':
        return ('Water', 'avoid')
    if tags.get('waterway') in ('river', 'canal'):
        return ('River / canal', 'avoid')
    if tags.get('natural') == 'wood' or tags.get('landuse') == 'forest':
        return ('Forest', 'avoid')
    if tags.get('natural') == 'wetland':
        return ('Wetland', 'avoid')
    if tags.get('natural') in ('scree', 'bare_rock'):
        return (tags['natural'].capitalize(), 'avoid')
    if tags.get('landuse') in ('residential', 'industrial', 'commercial', 'retail'):
        return ('Built-up', 'avoid')
    if tags.get('power') == 'line':
        return ('Power line', 'avoid')
    return ('Other', 'caution')


def _overpass_around(lat: float, lon: float, radius_m: float) -> list[dict]:
    parts = []
    for frag, _, _ in _QUERIES:
        if frag.startswith('node'):
            base, rest = 'node', frag[4:]
        elif frag.startswith('way'):
            base, rest = 'way', frag[3:]
        else:
            continue
        parts.append(f'{base}{rest}(around:{radius_m:.0f},{lat:.6f},{lon:.6f});')
    q = '[out:json][timeout:25];(' + ''.join(parts) + ');out tags center 200;'
    r = requests.post(
        OVERPASS_URL,
        data={'data': q},
        headers={'User-Agent': USER_AGENT},
        timeout=HTTP_TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json().get('elements', [])


# ── Public API ─────────────────────────────────────────────────────────

def survey(lat: float, lon: float,
           radius_m: float = DEFAULT_RADIUS_M,
           use_cache: bool = True) -> dict:
    """Return landing-surface assessment for the area around (lat, lon).

    Result schema:
      { 'verdict': 'good'|'caution'|'avoid'|'unknown',
        'good': [labels...], 'caution': [...], 'avoid': [...],
        'cached': bool, 'error': str|None }
    """
    init_db()
    k = _key(lat, lon, radius_m)

    if use_cache:
        with _DB_LOCK, _conn() as c:
            row = c.execute(
                "SELECT * FROM landing_survey WHERE key=?", (k,)
            ).fetchone()
        if row and (time.time() - row['fetched_at']) < CACHE_TTL_S:
            try:
                feats = json.loads(row['features_json'])
            except json.JSONDecodeError:
                feats = {'good': [], 'caution': [], 'avoid': []}
            return {
                'verdict': row['verdict'],
                'good': feats.get('good', []),
                'caution': feats.get('caution', []),
                'avoid': feats.get('avoid', []),
                'cached': True,
                'error': None,
            }

    try:
        elements = _overpass_around(lat, lon, radius_m)
    except Exception as e:  # noqa: BLE001
        return {
            'verdict': 'unknown', 'good': [], 'caution': [], 'avoid': [],
            'cached': False, 'error': f'{type(e).__name__}: {e}',
        }

    good, caution, avoid = [], [], []
    for el in elements:
        tags = el.get('tags') or {}
        label, rating = _classify_tags(tags)
        bucket = good if rating == 'good' else (caution if rating == 'caution' else avoid)
        if label not in bucket:
            bucket.append(label)

    # Verdict logic (Turnback-style, conservative for heli LZ):
    #   - If any "good" surface tagged → good
    #   - Else if avoid features present → avoid
    #   - Else if only caution → caution
    #   - Else → unknown
    if good:
        verdict = 'good'
    elif avoid:
        verdict = 'avoid'
    elif caution:
        verdict = 'caution'
    else:
        verdict = 'unknown'

    feats = {'good': good, 'caution': caution, 'avoid': avoid}
    with _DB_LOCK, _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO landing_survey "
            "(key, lat, lon, radius_m, verdict, good_count, caution_count, avoid_count, "
            " features_json, fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (k, lat, lon, radius_m, verdict, len(good), len(caution), len(avoid),
             json.dumps(feats), int(time.time())),
        )

    return {
        'verdict': verdict,
        'good': good, 'caution': caution, 'avoid': avoid,
        'cached': False, 'error': None,
    }


def stats() -> dict:
    init_db()
    with _DB_LOCK, _conn() as c:
        total = c.execute("SELECT COUNT(*) AS n FROM landing_survey").fetchone()['n']
        by_v = {r['verdict']: r['n'] for r in c.execute(
            "SELECT verdict, COUNT(*) AS n FROM landing_survey GROUP BY verdict"
        ).fetchall()}
    return {'total_surveys': total, 'by_verdict': by_v}
