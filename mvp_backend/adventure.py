"""
Adventure Mode orchestrator
============================
Find "cool places to land" reachable from a chosen origin within a user's
time-aloft budget, ranked by vibe match, landing confidence, and ETE.

Pipeline
--------
1. Resolve origin (ICAO or @lat,lon).
2. Compute reachable radius from time_aloft_min × cruise speed (heli DB
   lookup if helicopter type given). Round-trip default = halve the budget.
3. Build bbox; ensure POI cache covers it for the requested vibes.
4. Pull POIs from cache.
5. For each POI, find best landing site (airport-backed > off-airport).
6. Score and rank.
7. Return top N.

Favorites are persisted alongside POIs in the same SQLite DB.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, asdict
from typing import List, Optional

from mvp_backend.planner import load_airports_solver, Airport
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.grid_astar import _haversine_nm
from mvp_backend import helicopter_db
from mvp_backend import poi_provider
from mvp_backend import landing_sites

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRTM_DIR = os.path.join(ROOT, "mvp_backend", "srtm_cache")
_FAV_DB = os.path.join(os.path.dirname(__file__), "adventure_favorites.sqlite")
_FAV_LOCK = threading.Lock()

# Reserve fraction baked into the reachable radius (wind, maneuvering, vfr
# margin). 0.85 means we use 85% of nominal range.
RESERVE_FRACTION = 0.85
DEFAULT_CRUISE_KT = 100.0


# ── Favorites store ────────────────────────────────────────────────────

def _fav_conn() -> sqlite3.Connection:
    c = sqlite3.connect(_FAV_DB, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def _init_fav_db() -> None:
    with _FAV_LOCK, _fav_conn() as c:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                user_key  TEXT NOT NULL,
                poi_id    TEXT NOT NULL,
                payload   TEXT NOT NULL,    -- full JSON snapshot for offline use
                added_at  INTEGER NOT NULL,
                PRIMARY KEY (user_key, poi_id)
            )
            """
        )


def add_favorite(user_key: str, poi_id: str, payload: dict) -> dict:
    _init_fav_db()
    with _FAV_LOCK, _fav_conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO favorites (user_key, poi_id, payload, added_at) "
            "VALUES (?,?,?,?)",
            (user_key, poi_id, json.dumps(payload), int(time.time())),
        )
    return {"ok": True, "poi_id": poi_id}


def remove_favorite(user_key: str, poi_id: str) -> dict:
    _init_fav_db()
    with _FAV_LOCK, _fav_conn() as c:
        c.execute("DELETE FROM favorites WHERE user_key=? AND poi_id=?",
                  (user_key, poi_id))
    return {"ok": True, "poi_id": poi_id}


def list_favorites(user_key: str) -> List[dict]:
    _init_fav_db()
    with _FAV_LOCK, _fav_conn() as c:
        rows = c.execute(
            "SELECT poi_id, payload, added_at FROM favorites "
            "WHERE user_key=? ORDER BY added_at DESC",
            (user_key,),
        ).fetchall()
    out = []
    for r in rows:
        try:
            data = json.loads(r["payload"])
        except json.JSONDecodeError:
            data = {}
        data["_added_at"] = r["added_at"]
        out.append(data)
    return out


# ── Origin resolver ────────────────────────────────────────────────────

def _resolve_origin(code: str, airports: dict) -> Optional[Airport]:
    code = (code or "").strip()
    if not code:
        return None
    if code.startswith("@"):
        try:
            parts = code[1:].split(",")
            lat, lon = float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            return None
        provider = SRTMProvider(cache_dir=_SRTM_DIR)
        elev_m = provider.get_many_m([(lat, lon)])[0]
        elev_ft = float(meters_to_feet(elev_m)) if elev_m == elev_m else 0.0
        return Airport(icao=code, name=f"Custom ({lat:.4f},{lon:.4f})",
                       lat=lat, lon=lon, elevation_ft=elev_ft,
                       facility_use="", fuel_100ll=0, fuel_jeta=0)
    return airports.get(code.upper())


# ── Range / bbox ───────────────────────────────────────────────────────

def reachable_radius_nm(time_aloft_min: float, cruise_kt: float,
                        round_trip: bool) -> float:
    """One-way radius from total time-aloft budget."""
    one_way_min = (time_aloft_min / 2.0) if round_trip else time_aloft_min
    nominal = (one_way_min / 60.0) * cruise_kt
    return nominal * RESERVE_FRACTION


def _bbox_around(lat: float, lon: float, radius_nm: float) -> tuple[float, float, float, float]:
    dlat = radius_nm / 60.0
    dlon = radius_nm / max(1e-3, 60.0 * math.cos(math.radians(lat)))
    return (lat - dlat, lon - dlon, lat + dlat, lon + dlon)  # s,w,n,e


# ── Scoring ────────────────────────────────────────────────────────────

_CONFIDENCE_SCORE = {"green": 1.0, "amber": 0.55, "red": 0.0}
_VIBE_WEIGHT = 1.0
_ETE_WEIGHT = 0.4    # per fraction of max range used
_LANDING_WEIGHT = 1.2


def _score(distance_nm: float, max_radius_nm: float,
           landing_conf: str, vibe_match: bool) -> float:
    ete_frac = min(1.0, distance_nm / max_radius_nm) if max_radius_nm > 0 else 1.0
    return (
        _VIBE_WEIGHT * (1.0 if vibe_match else 0.5)
        + _LANDING_WEIGHT * _CONFIDENCE_SCORE.get(landing_conf, 0.0)
        - _ETE_WEIGHT * ete_frac
    )


# ── Main entry point ───────────────────────────────────────────────────

@dataclass
class AdventureRequest:
    origin: str
    time_aloft_min: float = 60.0
    vibes: List[str] = None
    helicopter_type: str = ""
    cruise_kt: float = 0.0    # override; 0 = derive from heli DB or default
    round_trip: bool = True
    limit: int = 25
    auto_fetch: bool = True   # if False, only read from cache
    # Performance inputs (for DA-adjusted cruise/burn and fuel range cap)
    oat_c: float = 15.0
    gross_weight_lb: float = 0.0   # 0 = use heli max GW
    usable_fuel_gal: float = 0.0   # 0 = use heli usable fuel; if both 0 → no fuel cap
    fuel_burn_gph: float = 0.0     # override; 0 = derive from heli DB
    reserve_min: float = 30.0


def _perf_at_origin(heli, origin, oat_c: float,
                    gross_weight_lb: float) -> Optional[dict]:
    """Density-altitude-corrected performance at the origin elevation.

    Returns None if no helicopter selected. Treats field elevation as
    pressure altitude (good enough for adventure-radius planning).
    """
    if heli is None:
        return None
    gw = gross_weight_lb if gross_weight_lb > 0 else None
    return heli.performance_at(origin.elevation_ft, oat_c, gw)


def _fuel_radius_nm(usable_fuel_gal: float, reserve_min: float,
                    burn_gph: float, cruise_kt: float,
                    round_trip: bool) -> Optional[float]:
    """Max one-way reach given usable fuel and reserve. None if any input ≤ 0."""
    if usable_fuel_gal <= 0 or burn_gph <= 0 or cruise_kt <= 0:
        return None
    reserve_gal = (reserve_min / 60.0) * burn_gph
    flyable_gal = max(0.0, usable_fuel_gal - reserve_gal)
    endurance_hr = flyable_gal / burn_gph
    total_nm = endurance_hr * cruise_kt
    one_way = (total_nm / 2.0) if round_trip else total_nm
    return max(0.0, one_way * RESERVE_FRACTION)



def search(req: AdventureRequest) -> dict:
    """Find ranked adventure spots. Synchronous; may take 10-30s if cache cold."""
    airports = load_airports_solver()
    origin = _resolve_origin(req.origin, airports)
    if origin is None:
        return {"error": f"Could not resolve origin '{req.origin}'", "results": []}

    # ── Aircraft + performance ──────────────────────────────────────
    heli = helicopter_db.get_helicopter(req.helicopter_type) if req.helicopter_type else None
    perf = _perf_at_origin(heli, origin, req.oat_c, req.gross_weight_lb)

    # Cruise: explicit override > DA-adjusted heli perf > heli sea-level > default.
    cruise = req.cruise_kt
    if cruise <= 0 and perf and perf.get("cruise_ktas"):
        cruise = float(perf["cruise_ktas"])
    if cruise <= 0 and heli and heli.perf_table:
        cruise = float(heli.perf_table[0].cruise_ktas)
    if cruise <= 0:
        cruise = DEFAULT_CRUISE_KT

    # Burn: explicit override > DA-adjusted heli perf > sea-level > none.
    burn = req.fuel_burn_gph
    if burn <= 0 and perf and perf.get("fuel_burn_gph"):
        burn = float(perf["fuel_burn_gph"])
    if burn <= 0 and heli and heli.perf_table:
        burn = float(heli.perf_table[0].fuel_burn_gph)

    # Usable fuel: explicit override > heli max usable.
    usable_fuel = req.usable_fuel_gal
    if usable_fuel <= 0 and heli:
        usable_fuel = float(heli.usable_fuel_gal)

    time_radius = reachable_radius_nm(req.time_aloft_min, cruise, req.round_trip)
    fuel_radius = _fuel_radius_nm(usable_fuel, req.reserve_min, burn,
                                  cruise, req.round_trip)

    # Effective radius is the binding constraint.
    if fuel_radius is None:
        radius_nm = time_radius
        binding = "time"
    else:
        if fuel_radius < time_radius:
            radius_nm = fuel_radius
            binding = "fuel"
        else:
            radius_nm = time_radius
            binding = "time"

    if radius_nm <= 0:
        return {"error": "No reachable range — check time, fuel, or aircraft.",
                "results": []}

    south, west, north, east = _bbox_around(origin.lat, origin.lon, radius_nm)

    vibes = req.vibes or list(poi_provider.VALID_VIBES)
    vibes = [v for v in vibes if v in poi_provider.VALID_VIBES]

    fetch_stats = {"fetched": 0, "cells_skipped": 0, "errors": []}
    if req.auto_fetch:
        fetch_stats = poi_provider.ensure_region(south, west, north, east, vibes)

    pois = poi_provider.query(south, west, north, east, vibes, limit=2000)

    provider = SRTMProvider(cache_dir=_SRTM_DIR)
    candidates = []
    for p in pois:
        d = _haversine_nm(origin.lat, origin.lon, p.lat, p.lon)
        if d > radius_nm:
            continue
        site = landing_sites.classify_landing(
            p.lat, p.lon, p.tags, airports, provider=provider,
        )
        score = _score(d, radius_nm, site.confidence, vibe_match=True)
        candidates.append({
            "poi": p.to_dict(),
            "distance_nm": round(d, 1),
            "ete_min_one_way": round((d / cruise) * 60.0, 1) if cruise > 0 else None,
            "fuel_gal_one_way": round((d / cruise) * burn, 2) if (cruise > 0 and burn > 0) else None,
            "landing": site.to_dict(),
            "score": round(score, 4),
        })

    candidates.sort(key=lambda r: r["score"], reverse=True)
    candidates = candidates[: max(1, req.limit)]

    return {
        "origin": {"icao": origin.icao, "lat": origin.lat, "lon": origin.lon,
                   "name": origin.name, "elevation_ft": origin.elevation_ft},
        "aircraft": {
            "type_code": getattr(heli, "type_code", ""),
            "name": getattr(heli, "name", ""),
            "airframe_class": getattr(heli, "airframe_class", ""),
            "selected": heli is not None,
        },
        "performance": perf,  # full snapshot or None
        "cruise_kt": round(cruise, 1),
        "fuel_burn_gph": round(burn, 2) if burn > 0 else None,
        "usable_fuel_gal": round(usable_fuel, 1) if usable_fuel > 0 else None,
        "reserve_min": req.reserve_min,
        "time_radius_nm": round(time_radius, 1),
        "fuel_radius_nm": round(fuel_radius, 1) if fuel_radius is not None else None,
        "radius_nm": round(radius_nm, 1),
        "radius_binding": binding,   # "time" or "fuel"
        "round_trip": req.round_trip,
        "bbox": {"south": south, "west": west, "north": north, "east": east},
        "vibes": vibes,
        "fetch": fetch_stats,
        "count": len(candidates),
        "results": candidates,
    }

