"""
Landing Site Classifier — Adventure Mode
=========================================
Given a "cool spot" POI, find the best place to put skids down nearby and
score how confidently we can recommend it.

Confidence tiers
----------------
- green  : public airport / heliport in NASR within ~3 nm
- amber  : off-airport landing on terrain that LOOKS suitable (gentle slope,
           no nearby obstacles, not in a hard-restricted area), but pilot
           must confirm legality with land manager.
- red    : nearest-known landing exists but with a hard blocker
           (Wilderness, National Park, slope > threshold, or no candidate
           within reach at all).

This module is intentionally conservative — when in doubt, downgrade.
The pilot is always the final authority.
"""

from __future__ import annotations

import math
import os
import sqlite3
from dataclasses import dataclass, asdict
from typing import List, Optional

from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.grid_astar import _haversine_nm
from mvp_backend import landing_surveyor

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRTM_DIR = os.path.join(ROOT, "mvp_backend", "srtm_cache")
_OBS_DB = os.path.join(os.path.dirname(__file__), "obstacle_data", "obstacles.sqlite")

# Slope sample radius for off-airport candidates (meters).
SLOPE_SAMPLE_RADIUS_M = 60.0
# Hard slope reject threshold (degrees) — heli skid landings beyond this
# are unsafe in a non-emergency context.
HARD_SLOPE_DEG = 8.0
# Obstacle rejection ring (nm) and minimum AGL height (ft) considered.
OBSTACLE_RING_NM = 0.25      # ~1500 ft
OBSTACLE_MIN_AGL_FT = 50

# OSM tags that flag legal NO-LAND areas (Wilderness Act, NPS, etc.).
_HARD_NO_LAND_TAGS = {
    ("boundary", "national_park"),
    ("boundary", "protected_area"),  # check protect_class below
    ("leisure", "nature_reserve"),
    ("landuse", "military"),
}
# Protect classes (IUCN) that disallow landings.
_HARD_PROTECT_CLASSES = {"1a", "1b", "2", "3"}  # strict reserves, wilderness, NP


@dataclass
class LandingSite:
    kind: str            # "airport" | "heliport" | "off_airport"
    name: str
    lat: float
    lon: float
    elevation_ft: float
    distance_to_poi_nm: float
    confidence: str      # "green" | "amber" | "red"
    reason: str          # short human string ("Public airport 1.2 nm away")
    icao: Optional[str] = None   # if backed by an airport DB entry
    notes: List[str] = None      # caveats

    def to_dict(self) -> dict:
        d = asdict(self)
        if d["notes"] is None:
            d["notes"] = []
        return d


# ── Slope estimation ───────────────────────────────────────────────────

_NM_PER_DEG_LAT = 60.0


def _offset_latlon(lat: float, lon: float, dx_m: float, dy_m: float) -> tuple[float, float]:
    """Approximate offset (dx east, dy north) in meters → new lat/lon."""
    dlat = (dy_m / 111320.0)
    dlon = (dx_m / (111320.0 * math.cos(math.radians(lat)))) if abs(lat) < 89.9 else 0
    return (lat + dlat, lon + dlon)


def estimate_slope_deg(lat: float, lon: float,
                       provider: Optional[SRTMProvider] = None,
                       sample_radius_m: float = SLOPE_SAMPLE_RADIUS_M) -> float:
    """Estimate terrain slope (degrees) by sampling SRTM in a small ring."""
    p = provider or SRTMProvider(cache_dir=_SRTM_DIR)
    samples = []
    # 4-point cardinal ring + center.
    pts = [
        (lat, lon),
        _offset_latlon(lat, lon, sample_radius_m, 0),
        _offset_latlon(lat, lon, -sample_radius_m, 0),
        _offset_latlon(lat, lon, 0, sample_radius_m),
        _offset_latlon(lat, lon, 0, -sample_radius_m),
    ]
    elevs = p.get_many_m(pts)
    valid = [e for e in elevs if e == e]  # filter NaN
    if len(valid) < 3:
        return float("nan")
    drop = max(valid) - min(valid)
    run = sample_radius_m * 2.0
    return math.degrees(math.atan2(drop, run))


# ── Airport-backed candidates ──────────────────────────────────────────

def _nearest_airports(poi_lat: float, poi_lon: float,
                      airports: dict, max_nm: float = 3.0,
                      limit: int = 3) -> List[tuple[float, object]]:
    out = []
    for ap in airports.values():
        d = _haversine_nm(poi_lat, poi_lon, ap.lat, ap.lon)
        if d <= max_nm:
            out.append((d, ap))
    out.sort(key=lambda x: x[0])
    return out[:limit]


# Wider fallback search radius (nm) when no close airport is found.
# Used to surface a "land at X, drive/hike to spot" suggestion instead of
# leaving the user with nothing.
FALLBACK_AIRPORT_NM = 25.0


def _nearest_airport_any(poi_lat: float, poi_lon: float,
                         airports: dict,
                         max_nm: float = FALLBACK_AIRPORT_NM):
    best = None
    best_d = max_nm
    for ap in airports.values():
        d = _haversine_nm(poi_lat, poi_lon, ap.lat, ap.lon)
        if d < best_d:
            best = ap
            best_d = d
    return (best_d, best) if best is not None else None


# ── Obstacle DOF lookup ────────────────────────────────────────────────

def _nearby_obstacles(lat: float, lon: float, ring_nm: float = OBSTACLE_RING_NM,
                      min_agl_ft: int = OBSTACLE_MIN_AGL_FT) -> list[dict]:
    if not os.path.exists(_OBS_DB):
        return []
    dlat = ring_nm / 60.0
    dlon = ring_nm / max(1e-3, 60.0 * math.cos(math.radians(lat)))
    try:
        c = sqlite3.connect(_OBS_DB, timeout=5)
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT lat, lon, agl, amsl, type, lit FROM obstacles "
            "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? AND agl >= ? "
            "LIMIT 50",
            (lat - dlat, lat + dlat, lon - dlon, lon + dlon, min_agl_ft),
        ).fetchall()
        c.close()
    except sqlite3.Error:
        return []
    out = []
    for r in rows:
        d = _haversine_nm(lat, lon, r["lat"], r["lon"])
        if d <= ring_nm:
            out.append({"distance_nm": round(d, 2), "agl_ft": r["agl"],
                        "type": r["type"], "lit": bool(r["lit"])})
    out.sort(key=lambda x: x["distance_nm"])
    return out


# ── Public API ─────────────────────────────────────────────────────────

def classify_landing(poi_lat: float, poi_lon: float, poi_tags: dict,
                     airports: dict,
                     provider: Optional[SRTMProvider] = None,
                     heli=None,
                     oat_c: float = 15.0,
                     gross_weight_lb: float = 0.0,
                     run_survey: bool = True) -> LandingSite:
    """Return the best LandingSite recommendation for a POI.

    Heli + OAT + GW (when provided) trigger a HOGE-at-site check.
    `run_survey=False` skips the Overpass surface survey (faster, less safe).
    """
    p = provider or SRTMProvider(cache_dir=_SRTM_DIR)

    # 1. Hard-reject if POI itself is tagged as a no-land area.
    no_land_reason = _check_no_land_tags(poi_tags)

    # 2. Public airport / heliport within 3 nm → green.
    near = _nearest_airports(poi_lat, poi_lon, airports)
    if near:
        d, ap = near[0]
        return LandingSite(
            kind="heliport" if "H" in (getattr(ap, "facility_use", "") or "") else "airport",
            name=getattr(ap, "name", ap.icao),
            lat=ap.lat, lon=ap.lon,
            elevation_ft=float(getattr(ap, "elevation_ft", 0) or 0),
            distance_to_poi_nm=round(d, 2),
            confidence="green",
            reason=f"Public airport {ap.icao} {d:.1f} nm from spot",
            icao=ap.icao,
            notes=[],
        )

    # 3. No airport nearby → consider an off-airport landing AT the POI.
    elev_m = p.get_many_m([(poi_lat, poi_lon)])[0]
    elev_ft = float(meters_to_feet(elev_m)) if elev_m == elev_m else 0.0
    slope = estimate_slope_deg(poi_lat, poi_lon, provider=p)

    notes: List[str] = []
    confidence = "amber"
    reason = "Off-airport landing — confirm legality & surface before committing"

    # 3a. Legal hard-no.
    if no_land_reason:
        confidence = "red"
        reason = no_land_reason
        notes.append("Federal law / regulation prohibits landings here.")
    # 3b. Slope.
    elif math.isnan(slope):
        confidence = "red"
        reason = "No terrain data — unable to assess landing surface"
    elif slope > HARD_SLOPE_DEG:
        confidence = "red"
        reason = f"Terrain slope ~{slope:.1f}° exceeds {HARD_SLOPE_DEG}° limit"
    else:
        notes.append(f"Estimated slope ~{slope:.1f}°")

    # 3c. OSM surface survey (Turnback-style: good/caution/avoid).
    survey = None
    if confidence == "amber" and run_survey:
        try:
            survey = landing_surveyor.survey(poi_lat, poi_lon)
        except Exception:  # noqa: BLE001
            survey = None
        if survey is not None:
            v = survey.get("verdict", "unknown")
            if v == "avoid":
                confidence = "red"
                reason = (f"Surface survey: {', '.join(survey.get('avoid', [])) or 'unlandable terrain'}")
            elif v == "good":
                surfaces = ', '.join(survey['good'][:3])
                reason = f"Off-airport — nearby surface looks landable ({surfaces})"
                if survey.get("caution"):
                    notes.append("Caution near LZ: " + ', '.join(survey['caution'][:3]))
            elif v == "caution":
                reason = "Off-airport — mixed surface (" + ', '.join(survey['caution'][:3]) + ")"
            else:  # unknown
                notes.append("No OSM surface tags within 200 m — scout via satellite first.")

    # 3d. Obstacle DOF check within ~1500 ft.
    if confidence == "amber":
        obs = _nearby_obstacles(poi_lat, poi_lon)
        if obs:
            confidence = "red"
            o = obs[0]
            reason = (f"DOF obstacle {o['agl_ft']} ft AGL ({o['type']}) "
                      f"{o['distance_nm']*6076:.0f} ft from spot")
            notes.append(f"{len(obs)} obstacles within {OBSTACLE_RING_NM} nm.")

    # 3e. HOGE check at site elevation (if heli + GW supplied).
    if confidence == "amber" and heli is not None and gross_weight_lb > 0 \
            and getattr(heli, "airframe_class", "helicopter") == "helicopter":
        try:
            site_perf = heli.performance_at(elev_ft, oat_c, gross_weight_lb)
            if site_perf and site_perf.get("can_hover_oge") is False:
                confidence = "red"
                reason = (f"Cannot HOGE at site — DA {site_perf.get('density_alt_ft')} ft, "
                          f"GW {int(gross_weight_lb)} lb exceeds HOGE limit")
                for w in (site_perf.get("warnings") or [])[:1]:
                    notes.append(w)
        except Exception:  # noqa: BLE001
            pass

    # If off-airport is red, fall back to the nearest airport within a wider
    # radius so the pilot always has at least one actionable option.
    if confidence == "red":
        fb = _nearest_airport_any(poi_lat, poi_lon, airports)
        if fb is not None:
            d, ap = fb
            return LandingSite(
                kind="heliport" if "H" in (getattr(ap, "facility_use", "") or "") else "airport",
                name=getattr(ap, "name", ap.icao),
                lat=ap.lat, lon=ap.lon,
                elevation_ft=float(getattr(ap, "elevation_ft", 0) or 0),
                distance_to_poi_nm=round(d, 1),
                confidence="amber",
                reason=(f"Land at {ap.icao} ({d:.1f} nm from spot); "
                        f"on-site landing rejected — {reason.lower()}"),
                icao=ap.icao,
                notes=["Plan ground transport from airport to the spot."] + notes,
            )

    return LandingSite(
        kind="off_airport",
        name="Off-airport spot",
        lat=poi_lat, lon=poi_lon,
        elevation_ft=elev_ft,
        distance_to_poi_nm=0.0,
        confidence=confidence,
        reason=reason,
        icao=None,
        notes=notes,
    )


def _check_no_land_tags(tags: dict) -> Optional[str]:
    if not tags:
        return None
    for k, v in tags.items():
        if (k, v) in _HARD_NO_LAND_TAGS:
            if k == "boundary" and v == "protected_area":
                pc = (tags.get("protect_class") or "").strip().lower()
                if pc in _HARD_PROTECT_CLASSES:
                    return f"Inside protected area (IUCN class {pc}) — landings prohibited"
                return None  # other protect classes: don't hard-reject
            if k == "boundary" and v == "national_park":
                return "Inside National Park — landings prohibited"
            if k == "leisure" and v == "nature_reserve":
                return "Inside nature reserve — landings typically prohibited"
            if k == "landuse" and v == "military":
                return "Military land — landings prohibited"
    if (tags.get("wilderness") or "").lower() in ("yes", "designated"):
        return "Designated Wilderness — Wilderness Act prohibits landings"
    return None
