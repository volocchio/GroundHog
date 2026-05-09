from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import sqlite3
import subprocess
import threading
import time

import logging

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import List

logger = logging.getLogger("groundhog")

from mvp_backend.planner import load_airports_solver, plan_route_multi_stop, terrain_avoid_leg, terrain_avoid_leg_streaming, leg_fuel_ok, plan_stop_sequence, plan_stop_sequences, Airport
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend.grid_astar import _haversine_nm
from mvp_backend import route_cache
from mvp_backend import terrain_intel
from mvp_backend import helicopter_db
from mvp_backend import landing_areas as _landing_areas
from mvp_backend import landcover as _landcover
from mvp_backend import tfrs as _tfrs

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


route_cache.init_db()

app = FastAPI(title="Terrain+Fuel Route Planner MVP")


@app.get("/health")
def health():
    """Health check endpoint for Docker / monitoring."""
    srtm_count = len([f for f in os.listdir(os.path.join(ROOT, "mvp_backend", "srtm_cache"))
                      if f.endswith(".hgt")]) if os.path.isdir(os.path.join(ROOT, "mvp_backend", "srtm_cache")) else 0
    return {"status": "ok", "srtm_tiles": srtm_count}


@app.get("/elevation")
def elevation(lat: float = Query(..., ge=-90, le=90),
              lon: float = Query(..., ge=-180, le=180)):
    """Return SRTM ground elevation (ft MSL) for a single lat/lon.

    Used by the live-flight HUD to compute AGL from the phone GPS altitude.
    """
    from mvp_backend.terrain_provider import meters_to_feet
    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
    elev_m = provider.get_many_m([(lat, lon)])[0]
    if elev_m != elev_m:  # NaN → ocean / void
        return {"lat": lat, "lon": lon, "elevation_ft": None, "elevation_m": None}
    return {
        "lat": lat,
        "lon": lon,
        "elevation_m": float(elev_m),
        "elevation_ft": float(meters_to_feet(elev_m)),
    }


def _point_in_ring(lat: float, lon: float, ring: list) -> bool:
    """Standard ray-casting point-in-polygon. ring = [[lat,lon], ...]."""
    n = len(ring)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        yi, xi = ring[i][0], ring[i][1]
        yj, xj = ring[j][0], ring[j][1]
        if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside


def _compute_leg_exposure(coords: list, landcover_features: list | None) -> dict:
    """Walk a leg's coords (plus SRTM) and return % distance over each
    landcover class plus % over water. Used by the UI risk-management panel.

    Returns a dict like:
      {"total_nm": 12.4, "water_pct": 18.2, "forest_pct": 41.0,
       "urban_pct": 3.1, "road_corridor_pct": 8.6, "open_pct": 0.0}

    Numbers are mid-segment samples, weighted by segment length. Roads use
    a 0.005° (~0.3 nm) tube around the polyline since they are linear
    features, not polygons. Water is sampled via SRTM (e <= 0 = ocean,
    NaN = void → not counted). Inland lakes are detected only if the
    landcover query returned an `open` polygon over them, which it
    usually doesn't — true inland lake detection lives in the planner's
    _detect_water_grid. For exposure stats we accept this limitation and
    flag SRTM-detected ocean water only.
    """
    if not coords or len(coords) < 2:
        return {}
    # Bin features by kind for cheap iteration.
    polys_by_kind: dict[str, list] = {"forest": [], "urban": [], "open": []}
    roads: list[list] = []
    for f in (landcover_features or []):
        kind = f.get("kind")
        ring = f.get("ring") or []
        if kind == "road":
            if len(ring) >= 2:
                roads.append(ring)
        elif kind in polys_by_kind and len(ring) >= 3:
            polys_by_kind[kind].append(ring)

    # Pre-build SRTM provider lazily (only if we are going to sample water).
    provider = None
    try:
        provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
    except Exception:
        provider = None

    ROAD_HALO_DEG = 0.005   # ~0.3 nm tube around the road polyline
    totals = {"forest": 0.0, "urban": 0.0, "open": 0.0,
              "road_corridor": 0.0, "water": 0.0}
    total_nm = 0.0

    for k in range(len(coords) - 1):
        lat0, lon0 = coords[k][0], coords[k][1]
        lat1, lon1 = coords[k + 1][0], coords[k + 1][1]
        seg_nm = _haversine_nm(lat0, lon0, lat1, lon1)
        if seg_nm <= 0:
            continue
        mlat = 0.5 * (lat0 + lat1)
        mlon = 0.5 * (lon0 + lon1)
        total_nm += seg_nm

        # Polygon hit-tests (mutually NOT exclusive — a forest can sit
        # inside an "open" landuse polygon; we record both).
        for kind, polys in polys_by_kind.items():
            for ring in polys:
                if _point_in_ring(mlat, mlon, ring):
                    totals[kind] += seg_nm
                    break

        # Road tube test: any road segment whose endpoints bracket the
        # midpoint within ROAD_HALO_DEG counts.
        on_road = False
        for r in roads:
            for i in range(len(r) - 1):
                ay, ax = r[i][0], r[i][1]
                by, bx = r[i + 1][0], r[i + 1][1]
                # Quick bbox reject
                if (mlat < min(ay, by) - ROAD_HALO_DEG
                        or mlat > max(ay, by) + ROAD_HALO_DEG
                        or mlon < min(ax, bx) - ROAD_HALO_DEG
                        or mlon > max(ax, bx) + ROAD_HALO_DEG):
                    continue
                # Distance from (mlat,mlon) to segment (a,b) in degree-space.
                dx, dy = bx - ax, by - ay
                if dx == 0 and dy == 0:
                    continue
                t = max(0.0, min(1.0, ((mlon - ax) * dx + (mlat - ay) * dy)
                                       / (dx * dx + dy * dy)))
                px, py = ax + t * dx, ay + t * dy
                if abs(px - mlon) <= ROAD_HALO_DEG and abs(py - mlat) <= ROAD_HALO_DEG:
                    on_road = True
                    break
            if on_road:
                break
        if on_road:
            totals["road_corridor"] += seg_nm

        # Water sample (SRTM ocean voids only — see docstring caveat).
        if provider is not None:
            try:
                e = provider.get_many_m([(mlat, mlon)])[0]
                if e == e and e <= 0.0:   # not NaN AND at/below sea level
                    totals["water"] += seg_nm
            except Exception:
                pass

    if total_nm <= 0:
        return {}
    out = {"total_nm": round(total_nm, 2)}
    for k, nm in totals.items():
        out[f"{k}_pct"] = round(100.0 * nm / total_nm, 1)
    return out


def _density_altitude_ft(pressure_alt_ft: float, oat_c: float) -> float:
    """ISA-deviation density altitude approximation.

    DA = PA + 120 ft per °C above ISA at that pressure altitude.
    ISA temp at PA: 15 \u2212 1.98 \u00d7 PA / 1000 (\u00b0C).

    Good to ~50 ft for any altitude a piston heli will see; the textbook
    formula has a barometric pressure term we don't have at planning time.
    """
    isa_c = 15.0 - 1.98 * (pressure_alt_ft / 1000.0)
    return pressure_alt_ft + 120.0 * (oat_c - isa_c)


def _da_adjusted_climb_fpm(req: "RouteRequest", heli) -> float:
    """Scale the user's max_climb_fpm down for density-altitude effects.

    Piston-helicopter ROC degrades roughly linearly with DA from sea-level
    rated value to zero at the absolute (DA) ceiling. We use:

      factor = max(0.20, 1 \u2212 cruise_DA / abs_ceiling_DA)

    Floor at 20% so we never zero-out the climb-rate edge constraint and
    accidentally make every cell impassable. The user is shown a warning
    in the perf panel when DA is high; the planner just gets a smaller
    ROC budget per edge, which forces it around steep climbs.

    Inputs:
      cruise_DA  derived from req.max_msl_ft (planning ceiling) and req.oat_c
      abs_ceil   helicopter_db.service_ceiling_da_ft if a heli was selected,
                 else 14000 ft (typical piston single).
    """
    if req.max_climb_fpm <= 0:
        return req.max_climb_fpm
    cruise_pa = max(0.0, req.max_msl_ft)
    cruise_da = _density_altitude_ft(cruise_pa, req.oat_c)
    abs_ceil = 14000.0
    if heli is not None and getattr(heli, "service_ceiling_da_ft", 0):
        abs_ceil = float(heli.service_ceiling_da_ft) or abs_ceil
    factor = 1.0 - (cruise_da / abs_ceil)
    if factor < 0.20:
        factor = 0.20
    if factor > 1.0:
        factor = 1.0
    return req.max_climb_fpm * factor


class RouteRequest(BaseModel):
    dep_icao: str
    arr_icao: str

    helicopter_type: str = Field(default="", description="Helicopter type code (e.g. S300C, R44, B206). Empty = manual entry.")
    oat_c: float = Field(default=15.0, description="Outside air temperature (°C) for performance calculations.")
    gross_weight_lb: float = Field(default=0, ge=0, description="Gross weight (lb). 0 = use max gross weight for type.")

    cruise_speed_kt: float = Field(gt=0)
    usable_fuel_gal: float = Field(gt=0)
    fuel_burn_gph: float = Field(gt=0)
    reserve_min: float = Field(ge=0)

    min_agl_ft: float = Field(ge=0)
    max_msl_ft: float = Field(gt=0)
    max_agl_ft: float = Field(default=0, ge=0)

    required_fuel: str = Field(pattern=r"^(100LL|JETA)$")

    max_detour_factor: float = Field(default=1.8, ge=1.0, le=5.0)

    max_climb_fpm: float = Field(default=0, ge=0)
    max_descent_fpm: float = Field(default=0, ge=0)

    climb_speed_kt: float = Field(default=0, ge=0)
    descent_speed_kt: float = Field(default=0, ge=0)

    terrain_follow: bool = Field(default=False)

    obstacle_radius_nm: float = Field(default=0.5, ge=0, le=5.0)
    obstacle_clearance_ft: float = Field(default=500, ge=0, le=2000)

    adsb_out: bool = Field(default=True)
    avoid_airspace: list[str] = Field(default_factory=lambda: ["P", "R"])
    avoid_borders: bool = Field(default=True)

    water_risk: float = Field(default=100, ge=0, le=100,
                              description="Water crossing risk tolerance: 0=must glide to shore, 100=ignore water.")
    has_floats: bool = Field(default=False,
                             description="Pop-out emergency floats (EFS) installed. Reduces water-cost penalty in planner.")
    use_landcover: bool = Field(default=False,
                                description="Use OSM landcover (roads/forest/urban) as a planner cost layer. Slower planning, smarter routes.")
    avoid_tfrs: bool = Field(default=True,
                             description="Hard-block active FAA TFRs in the planner. Falls back to advisory-only display if the FAA feed is unreachable.")
    slope_threshold_deg: float = Field(default=15, ge=5, le=45,
                                       description="Max acceptable landing slope in degrees.")
    enforce_slope: bool = Field(default=False,
                                description="If true, planner hard-blocks cells whose terrain slope exceeds slope_threshold_deg AND are beyond autorot glide reach of safer ground.")
    glide_ratio: float = Field(default=4.0, ge=0, le=20,
                               description="Autorotation glide ratio (horizontal:vertical).")

    fuel_load_gal: float = Field(default=0, ge=0,
                                 description="Actual fuel on board at departure (gal). 0 = use usable_fuel_gal.")

    waypoints: list[str] = Field(default_factory=list)


@app.get("/", response_class=HTMLResponse)
def index():
    # Simple single-file webapp
    import os
    here = os.path.dirname(__file__)
    with open(os.path.join(here, "webapp.html"), "r", encoding="utf-8") as f:
        return f.read()


# ── PWA assets ──────────────────────────────────────────────────────────
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/manifest.webmanifest")
def manifest():
    p = os.path.join(_STATIC_DIR, "manifest.webmanifest")
    return FileResponse(p, media_type="application/manifest+json")


@app.get("/sw.js")
def service_worker():
    p = os.path.join(_STATIC_DIR, "sw.js")
    # Service workers must be served with no-cache so updates roll out fast.
    return FileResponse(
        p,
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate",
                 "Service-Worker-Allowed": "/"},
    )


@app.get("/apple-touch-icon.png")
@app.get("/apple-touch-icon-precomposed.png")
def apple_touch_icon():
    return FileResponse(os.path.join(_STATIC_DIR, "apple-touch-icon.png"),
                        media_type="image/png")


@app.get("/favicon.ico")
def favicon():
    p = os.path.join(_STATIC_DIR, "icon-192.png")
    if os.path.isfile(p):
        return FileResponse(p, media_type="image/png")
    return Response(status_code=204)


# ── Map tile proxy + on-disk cache ───────────────────────────────
# Serving tiles same-origin so the service worker can cache them cleanly
# (cross-origin opaque responses count fully against the cache quota and
# can't be inspected). Also lets us cache once on the server and respect
# upstream tile-server ToS / rate limits.
#
# Provider key → (URL template, file extension, list of {s} subdomains or None)
# {z},{x},{y} are filled in per request.
_TILE_PROVIDERS = {
    "osm": ("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", "png", ["a", "b", "c"]),
    "opentopo": ("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", "png", ["a", "b", "c"]),
    "usgs-topo": ("https://basemap.nationalmap.gov/arcgis/rest/services/USGSTopo/MapServer/tile/{z}/{y}/{x}", "png", None),
    "usgs-imagery": ("https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryTopo/MapServer/tile/{z}/{y}/{x}", "jpg", None),
    "vfr-sectional": ("https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}", "png", None),
    "vfr-world": ("https://services.arcgisonline.com/arcgis/rest/services/Specialty/World_Navigation_Charts/MapServer/tile/{z}/{y}/{x}", "png", None),
}

_TILE_CACHE_DIR = os.path.join(ROOT, "mvp_backend", "tile_cache")
_TILE_USER_AGENT = "GroundHog/1.0 (+https://groundhog.voloaltro.tech)"
# Cap upstream fetches for safety. Browsers limit concurrent connections per
# origin already; this guards the disk cache from runaway prefetch loops.
_TILE_MAX_Z = 16
_tile_fetch_lock = threading.Semaphore(8)


@app.get("/tiles/{provider}/{z}/{x}/{y}.png")
@app.get("/tiles/{provider}/{z}/{x}/{y}.jpg")
def get_tile(provider: str, z: int, x: int, y: int):
    """Fetch a map tile from the upstream provider, cache it on disk, and
    return it. Same-origin URL so the service worker can cache it cleanly
    for offline use."""
    spec = _TILE_PROVIDERS.get(provider)
    if not spec:
        raise HTTPException(404, f"Unknown tile provider: {provider}")
    if z < 0 or z > _TILE_MAX_Z:
        raise HTTPException(400, f"Zoom {z} out of range (0..{_TILE_MAX_Z})")
    max_xy = (1 << z) - 1
    if x < 0 or x > max_xy or y < 0 or y > max_xy:
        raise HTTPException(400, "Tile x/y out of range for zoom")

    url_tpl, ext, subdomains = spec
    cache_path = os.path.join(_TILE_CACHE_DIR, provider, str(z), str(x), f"{y}.{ext}")
    media_type = "image/png" if ext == "png" else "image/jpeg"
    # Long Cache-Control: tiles are effectively immutable for our purposes
    # (chart cycles are months). SW + browser will cache aggressively.
    cache_headers = {"Cache-Control": "public, max-age=2592000, immutable"}

    if os.path.isfile(cache_path) and os.path.getsize(cache_path) > 0:
        return FileResponse(cache_path, media_type=media_type, headers=cache_headers)

    # Fetch upstream
    if subdomains:
        sub = subdomains[(x + y) % len(subdomains)]
        url = url_tpl.format(s=sub, z=z, x=x, y=y)
    else:
        url = url_tpl.format(z=z, x=x, y=y)

    import requests
    try:
        with _tile_fetch_lock:
            r = requests.get(url, headers={"User-Agent": _TILE_USER_AGENT}, timeout=15)
    except requests.RequestException as e:
        raise HTTPException(502, f"Upstream tile fetch failed: {e}")
    if r.status_code != 200 or not r.content:
        # Don't cache failures
        raise HTTPException(r.status_code if r.status_code >= 400 else 502,
                            f"Upstream returned {r.status_code}")

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    tmp_path = cache_path + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            f.write(r.content)
        os.replace(tmp_path, cache_path)
    except OSError as e:
        # If we can't write to cache, still serve the tile.
        logger.warning("Tile cache write failed for %s: %s", cache_path, e)
        try:
            os.remove(tmp_path)
        except OSError:
            pass
    return Response(content=r.content, media_type=media_type, headers=cache_headers)


@app.get("/health")
def health():
    return {"ok": True}


# ── GitHub webhook auto-deploy ───────────────────────────────────
DEPLOY_SCRIPT = os.path.join(ROOT, "deploy", "deploy.sh")
WEBHOOK_SECRET_FILE = os.path.join(ROOT, ".webhook_secret")


@app.post("/deploy-hook")
async def deploy_hook(request: Request):
    if not os.path.isfile(WEBHOOK_SECRET_FILE):
        raise HTTPException(404)
    secret = open(WEBHOOK_SECRET_FILE).read().strip().encode()
    body = await request.body()
    sig_header = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig_header, expected):
        raise HTTPException(403, "Invalid signature")
    subprocess.Popen(["bash", DEPLOY_SCRIPT], cwd=ROOT)
    return {"status": "deploying"}


@app.get("/airports")
def airports_list():
    """Return all airports for map display."""
    airports = load_airports_solver()
    seen = set()
    result = []
    for ap in airports.values():
        if id(ap) in seen:
            continue
        seen.add(id(ap))
        result.append({
            "icao": ap.icao,
            "name": ap.name,
            "lat": ap.lat,
            "lon": ap.lon,
            "elev": ap.elevation_ft,
            "facility_use": ap.facility_use,
            "fuel_100ll": ap.fuel_100ll,
            "fuel_jeta": ap.fuel_jeta,
        })
    return result


# ── helicopter performance database ─────────────────────────────────

@app.get("/helicopters")
def helicopters_list():
    """Return all helicopter types in the database."""
    return helicopter_db.list_helicopters()


@app.get("/helicopters/{type_code}")
def helicopter_detail(type_code: str):
    """Return full detail for a helicopter type."""
    heli = helicopter_db.get_helicopter(type_code)
    if not heli:
        raise HTTPException(404, f"Unknown helicopter type: {type_code}")
    return heli.to_dict()


@app.get("/helicopters/{type_code}/performance")
def helicopter_performance(
    type_code: str,
    pressure_alt_ft: float = Query(0),
    oat_c: float = Query(15),
    gross_weight_lb: float = Query(0),
):
    """Compute performance at given conditions."""
    heli = helicopter_db.get_helicopter(type_code)
    if not heli:
        raise HTTPException(404, f"Unknown helicopter type: {type_code}")
    gw = gross_weight_lb if gross_weight_lb > 0 else None
    return heli.performance_at(pressure_alt_ft, oat_c, gw)


@app.post("/route")
def route(req: RouteRequest):
    airports = load_airports_solver()
    dep = _resolve_airport(req.dep_icao, airports)
    arr = _resolve_airport(req.arr_icao, airports)
    if not dep:
        raise HTTPException(404, f"Unknown dep_icao {req.dep_icao}")
    if not arr:
        raise HTTPException(404, f"Unknown arr_icao {req.arr_icao}")

    route_cache.record_request(dep.icao, arr.icao, req.max_msl_ft, req.min_agl_ft, req.max_detour_factor,
                                 cruise_kt=req.cruise_speed_kt, fuel_gal=req.usable_fuel_gal,
                                 burn_gph=req.fuel_burn_gph, reserve_min=req.reserve_min,
                                 fuel_type=req.required_fuel,
                                 max_climb_fpm=req.max_climb_fpm, max_descent_fpm=req.max_descent_fpm,
                                 climb_speed_kt=req.climb_speed_kt, descent_speed_kt=req.descent_speed_kt)

    # ── Density-altitude ROC haircut ──
    # Scale the user's max_climb_fpm down for the planning ceiling's DA.
    # This makes hot/high days physically harder and forces the planner
    # around steeper terrain instead of magically climbing over it.
    _heli_for_da = helicopter_db.get_helicopter(req.helicopter_type) if req.helicopter_type else None
    _eff_climb_fpm = _da_adjusted_climb_fpm(req, _heli_for_da)

    result = plan_route_multi_stop(
        dep=dep,
        arr=arr,
        airports=airports,
        cruise_speed_kt=req.cruise_speed_kt,
        usable_fuel_gal=req.usable_fuel_gal,
        burn_gph=req.fuel_burn_gph,
        reserve_min=req.reserve_min,
        max_msl_ft=req.max_msl_ft,
        min_agl_ft=req.min_agl_ft,
        required_fuel=req.required_fuel,
        max_detour_factor=req.max_detour_factor,
        max_climb_fpm=_eff_climb_fpm,
        max_descent_fpm=req.max_descent_fpm,
        climb_speed_kt=req.climb_speed_kt,
        descent_speed_kt=req.descent_speed_kt,
        glide_ratio=req.glide_ratio,
        water_risk=req.water_risk,
        has_floats=req.has_floats,
        slope_threshold_deg=req.slope_threshold_deg,
        enforce_slope=req.enforce_slope,
    )
    return result


def _resolve_airport(code: str, airports: dict) -> Airport | None:
    """Resolve an ICAO code or @lat,lon string to an Airport object."""
    code = code.strip()
    if code.startswith('@'):
        # Custom lat/lon point: @lat,lon
        try:
            parts = code[1:].split(',')
            lat, lon = float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            return None
        from mvp_backend.terrain_provider import meters_to_feet
        provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
        elev_m = provider.get_many_m([(lat, lon)])[0]
        elev_ft = meters_to_feet(elev_m) if elev_m == elev_m else 0.0
        return Airport(
            icao=code,
            name=f"Custom ({lat:.4f}, {lon:.4f})",
            lat=lat,
            lon=lon,
            elevation_ft=elev_ft,
            facility_use="",
            fuel_100ll=0,
            fuel_jeta=0,
        )
    return airports.get(code.upper())


@app.post("/route/stream")
def route_stream(req: RouteRequest):
    """SSE endpoint that streams A* exploration events for visualization."""
    logger.info("POST /route/stream  %s → %s  msl=%.0f agl=%.0f detour=%.1f "
                "climb=%s desc=%s avoid=%s obstacles=%.1f/%.0f",
                req.dep_icao, req.arr_icao, req.max_msl_ft, req.min_agl_ft,
                req.max_detour_factor, req.max_climb_fpm, req.max_descent_fpm,
                req.avoid_airspace, req.obstacle_radius_nm, req.obstacle_clearance_ft)
    airports = load_airports_solver()
    dep = _resolve_airport(req.dep_icao, airports)
    arr = _resolve_airport(req.arr_icao, airports)
    if not dep:
        raise HTTPException(404, f"Unknown dep_icao {req.dep_icao}")
    if not arr:
        raise HTTPException(404, f"Unknown arr_icao {req.arr_icao}")

    # Resolve waypoints into airport objects; -NF suffix = no fuel, -VIA = flyover
    waypoint_aps = []
    waypoint_fuel = {}  # icao → True (refuel) / False (no fuel)
    waypoint_via = set()  # icao codes that are flyover-only (no landing)
    for wp_raw in (req.waypoints or []):
        raw = wp_raw.strip()
        if raw.upper().endswith('-VIA'):
            code = raw[:-4].strip()
            fuel = False
            via = True
        elif raw.upper().endswith('-NF'):
            code = raw[:-3].strip()
            fuel = False
            via = False
        else:
            code = raw
            fuel = True
            via = False
        wp = _resolve_airport(code, airports)
        if not wp:
            raise HTTPException(404, f"Unknown waypoint: {code}")
        waypoint_aps.append(wp)
        waypoint_fuel[wp.icao] = fuel
        if via:
            waypoint_via.add(wp.icao)

    # Segments: dep → wp1, wp1 → wp2, ..., wpN → arr
    segment_endpoints = [dep] + waypoint_aps + [arr]

    route_cache.record_request(dep.icao, arr.icao, req.max_msl_ft, req.min_agl_ft, req.max_detour_factor,
                                 cruise_kt=req.cruise_speed_kt, fuel_gal=req.usable_fuel_gal,
                                 burn_gph=req.fuel_burn_gph, reserve_min=req.reserve_min,
                                 fuel_type=req.required_fuel,
                                 max_climb_fpm=req.max_climb_fpm, max_descent_fpm=req.max_descent_fpm,
                                 climb_speed_kt=req.climb_speed_kt, descent_speed_kt=req.descent_speed_kt)

    def generate():
        # ── ADS-B compliance check ──
        if not req.adsb_out:
            all_aps = [dep] + waypoint_aps + [arr]
            hits = _airports_in_airspace(all_aps, classes=("B", "C"))
            if hits:
                yield f"data: {json.dumps({'type': 'adsb_warning', 'airports': hits})}\n\n"

        # ── Helicopter performance check (departure) ──
        heli = helicopter_db.get_helicopter(req.helicopter_type) if req.helicopter_type else None
        # Density-altitude-adjusted climb-rate budget for the planner.
        # See _da_adjusted_climb_fpm for derivation. Computed once up-front
        # because cruise ceiling and OAT don't change leg-to-leg.
        eff_climb_fpm = _da_adjusted_climb_fpm(req, heli)
        if req.max_climb_fpm > 0 and eff_climb_fpm < req.max_climb_fpm:
            yield (
                "data: "
                + json.dumps({
                    "type": "da_climb_haircut",
                    "requested_fpm": round(req.max_climb_fpm),
                    "effective_fpm": round(eff_climb_fpm),
                    "pct_of_requested": round(100.0 * eff_climb_fpm / req.max_climb_fpm, 1),
                })
                + "\n\n"
            )
        if heli:
            gw = req.gross_weight_lb if req.gross_weight_lb > 0 else None
            dep_perf = heli.performance_at(dep.elevation_ft, req.oat_c, gw)
            arr_perf = heli.performance_at(arr.elevation_ft, req.oat_c, gw)
            # Send helicopter info event with performance at departure & arrival
            heli_event = {
                "type": "helicopter_info",
                "helicopter": heli.to_dict(),
                "oat_c": req.oat_c,
                "departure": dep_perf,
                "arrival": arr_perf,
            }
            yield f"data: {json.dumps(heli_event)}\n\n"
            # If there are blockers at departure, warn but don't stop
            if dep_perf["warnings"]:
                yield f"data: {json.dumps({'type': 'helicopter_warning', 'phase': 'departure', 'airport': dep.icao, 'warnings': dep_perf['warnings']})}\n\n"
            if arr_perf["warnings"]:
                yield f"data: {json.dumps({'type': 'helicopter_warning', 'phase': 'arrival', 'airport': arr.icao, 'warnings': arr_perf['warnings']})}\n\n"

        # First pass: plan all segments to get total leg count and stops
        all_sequences = []
        blocked_pairs = set()
        seg_last_leg_dist = 0.0  # actual A* distance of last leg in prev segment
        # Track weight across legs for helicopter performance checks
        initial_weight = req.gross_weight_lb if req.gross_weight_lb > 0 else (
            heli.max_gross_weight_lb if heli else 0)
        current_weight = initial_weight
        fw_per_gal = heli.fuel_weight_lb_per_gal if heli else 6.0
        for si in range(len(segment_endpoints) - 1):
            seg_dep = segment_endpoints[si]
            seg_arr = segment_endpoints[si + 1]

            # Determine starting fuel: full if first segment or if we refuel here
            if si == 0 or waypoint_fuel.get(seg_dep.icao, True):
                start_fuel = (req.fuel_load_gal if si == 0 and req.fuel_load_gal > 0
                              else req.usable_fuel_gal)
            else:
                # No fuel at this waypoint — estimate remaining from last leg
                last_leg_time = seg_last_leg_dist / req.cruise_speed_kt if req.cruise_speed_kt > 0 else 0
                fuel_used = last_leg_time * req.fuel_burn_gph
                start_fuel = max(0.0, req.usable_fuel_gal - fuel_used)

            max_retries = 5
            segment_ok = False
            for attempt in range(max_retries + 1):
                # Escalate detour factor on retries (+0.15 per attempt)
                eff_detour = req.max_detour_factor + attempt * 0.15
                # Last-ditch: expand search budget
                eff_expansions = 5000 if attempt >= max_retries else 2000

                # Get up to 3 alternative stop sequences
                sequences_pool = plan_stop_sequences(
                    dep=seg_dep, arr=seg_arr, airports=airports,
                    cruise_speed_kt=req.cruise_speed_kt,
                    usable_fuel_gal=req.usable_fuel_gal,
                    start_fuel_gal=start_fuel,
                    burn_gph=req.fuel_burn_gph,
                    reserve_min=req.reserve_min,
                    required_fuel=req.required_fuel,
                    max_detour_factor=eff_detour,
                    blocked_pairs=blocked_pairs,
                    max_msl_ft=req.max_msl_ft,
                    min_agl_ft=req.min_agl_ft,
                    max_climb_fpm=req.max_climb_fpm,
                    max_descent_fpm=req.max_descent_fpm,
                    climb_speed_kt=req.climb_speed_kt,
                    descent_speed_kt=req.descent_speed_kt,
                    k=3,
                    max_expansions=eff_expansions,
                )

                if not sequences_pool:
                    if attempt < max_retries:
                        yield f"data: {json.dumps({'type': 'reroute', 'message': f'No fuel-feasible route for {seg_dep.icao} → {seg_arr.icao} (detour {eff_detour:.1f}x, attempt {attempt+1}/{max_retries+1}) — widening search...', 'blocked': [list(p) for p in blocked_pairs], 'keep_legs': sum(len(s) - 1 for s in all_sequences)})}\n\n"
                        continue
                    # All retries exhausted
                    yield f"data: {json.dumps({'type': 'no_path', 'message': f'No fuel-feasible route found for segment {seg_dep.icao} → {seg_arr.icao} after {max_retries+1} attempts (max detour {eff_detour:.1f}x).'})}\n\n"
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    return

                # Try each candidate sequence until one succeeds all legs
                seq_succeeded = False
                for seq_idx, sequence in enumerate(sequences_pool):
                    seg_num_legs = len(sequence) - 1
                    leg_offset = sum(len(s) - 1 for s in all_sequences)

                    # If we already attempted (and partially streamed) a prior
                    # candidate sequence in this segment, tell the client to
                    # discard those stale legs before we stream the new one.
                    # Without this the UI keeps the dead-end leg(s) appended
                    # to the legsList and renders nonsense like
                    # KSZT→KCOE  (orphan from failed seq #1)
                    # KSZT→KHRF  (real first leg of seq #2)
                    if seq_idx > 0:
                        yield (
                            "data: "
                            + json.dumps({
                                "type": "reroute",
                                "message": (
                                    f"Trying alternate fuel-stop sequence "
                                    f"({seq_idx + 1}/{len(sequences_pool)}) for "
                                    f"{seg_dep.icao} → {seg_arr.icao}\u2026"
                                ),
                                "keep_legs": leg_offset,
                            })
                            + "\n\n"
                        )

                    # Build flattened stops list for route_plan event
                    # Only include actual fuel stops (exclude -NF waypoints)
                    flat_stops = []
                    for prev_seq in all_sequences:
                        flat_stops.extend(ap.icao for ap in prev_seq[1:] if waypoint_fuel.get(ap.icao, True))
                    flat_stops.extend(ap.icao for ap in sequence[1:-1] if waypoint_fuel.get(ap.icao, True))
                    for fi in range(si + 1, len(segment_endpoints) - 1):
                        ep = segment_endpoints[fi + 1] if fi + 1 < len(segment_endpoints) - 1 else None
                        if ep and waypoint_fuel.get(ep.icao, True):
                            flat_stops.append(ep.icao)
                    flat_stops = [s for s in flat_stops if s and s != arr.icao]

                    total_legs_so_far = leg_offset + seg_num_legs
                    yield f"data: {json.dumps({'type': 'route_plan', 'stops': flat_stops, 'num_legs': total_legs_so_far})}\n\n"

                    leg_failed = False
                    last_fail_event = None
                    for i in range(seg_num_legs):
                        from_ap = sequence[i]
                        to_ap = sequence[i + 1]
                        global_leg = leg_offset + i

                        prev_pt = None
                        if i > 0:
                            prev_pt = (sequence[i - 1].lat, sequence[i - 1].lon)
                        elif si > 0:
                            prev_seq = all_sequences[-1] if all_sequences else None
                            if prev_seq and len(prev_seq) >= 2:
                                prev_pt = (prev_seq[-2].lat, prev_seq[-2].lon)

                        yield f"data: {json.dumps({'type': 'leg_start', 'from': from_ap.icao, 'to': to_ap.icao, 'leg_index': global_leg})}\n\n"

                        # Pre-fetch landcover features for this leg's bbox if requested.
                        # Use a generous margin so the planner can detour within the
                        # landcover-cost area. Cached aggressively in landcover.sqlite.
                        leg_lc_features = None
                        if req.use_landcover:
                            try:
                                margin = 0.25  # ~15 nm padding
                                lc_s = min(from_ap.lat, to_ap.lat) - margin
                                lc_n = max(from_ap.lat, to_ap.lat) + margin
                                lc_w = min(from_ap.lon, to_ap.lon) - margin
                                lc_e = max(from_ap.lon, to_ap.lon) + margin
                                lc_res = _landcover.query_bbox(lc_s, lc_n, lc_w, lc_e)
                                if lc_res and not lc_res.get('error'):
                                    leg_lc_features = lc_res.get('features') or []
                            except Exception:
                                leg_lc_features = None

                        # Pre-fetch active TFRs for this leg's bbox.
                        # 0.25\u00b0 margin matches landcover; cached for ~15 min.
                        # If the FAA feed is unreachable we get back [] and
                        # planning continues normally.
                        leg_tfrs = None
                        if req.avoid_tfrs:
                            try:
                                margin = 0.25
                                ts = min(from_ap.lat, to_ap.lat) - margin
                                tn = max(from_ap.lat, to_ap.lat) + margin
                                tw = min(from_ap.lon, to_ap.lon) - margin
                                te = max(from_ap.lon, to_ap.lon) + margin
                                leg_tfrs = _tfrs.get_active_tfrs(bbox=(ts, tw, tn, te)) or []
                                if leg_tfrs:
                                    _tfr_summary = [
                                        {"notam": t["notam"], "description": (t.get("description") or "")[:200]}
                                        for t in leg_tfrs
                                    ]
                                    yield (
                                        "data: "
                                        + json.dumps({
                                            "type": "tfr_warning",
                                            "leg_index": global_leg,
                                            "from": from_ap.icao,
                                            "to": to_ap.icao,
                                            "tfrs": _tfr_summary,
                                        })
                                        + "\n\n"
                                    )
                            except Exception:
                                leg_tfrs = None

                        for event in terrain_avoid_leg_streaming(
                            from_ap, to_ap,
                            max_msl_ft=req.max_msl_ft,
                            min_agl_ft=req.min_agl_ft,
                            max_detour_factor=eff_detour,
                            max_climb_fpm=eff_climb_fpm,
                            max_descent_fpm=req.max_descent_fpm,
                            cruise_speed_kt=req.cruise_speed_kt,
                            climb_speed_kt=req.climb_speed_kt,
                            descent_speed_kt=req.descent_speed_kt,
                            avoid_airspace=req.avoid_airspace or [],
                            obstacle_radius_nm=req.obstacle_radius_nm,
                            obstacle_clearance_ft=req.obstacle_clearance_ft,
                            prev_point=prev_pt,
                            avoid_borders=req.avoid_borders,
                            glide_ratio=req.glide_ratio,
                            water_risk=req.water_risk,
                            has_floats=req.has_floats,
                            slope_threshold_deg=req.slope_threshold_deg,
                            enforce_slope=req.enforce_slope,
                            use_landcover=req.use_landcover,
                            landcover_features=leg_lc_features,
                            tfr_polygons=leg_tfrs,
                        ):
                            if event.get("type") == "path":
                                event["leg_index"] = global_leg
                                event["from"] = from_ap.icao
                                event["to"] = to_ap.icao
                                # Mark VIA (flyover) waypoints
                                if from_ap.icao in waypoint_via:
                                    event["from_via"] = True
                                if to_ap.icao in waypoint_via:
                                    event["to_via"] = True
                                seg_last_leg_dist = event.get("dist_nm", 0.0)
                                # ── Per-leg exposure stats (risk panel) ──
                                # Always emit; landcover-derived stats are
                                # 0 when the user hasn't opted into landcover,
                                # but the SRTM-based water % is always useful.
                                try:
                                    event["exposure"] = _compute_leg_exposure(
                                        event.get("coords") or [], leg_lc_features,
                                    )
                                except Exception:
                                    event["exposure"] = {}
                                # ── Attach helicopter performance for this leg ──
                                if heli:
                                    max_terr = event.get("max_terrain_ft", max(from_ap.elevation_ft, to_ap.elevation_ft))
                                    leg_dist = event.get("dist_nm", 0.0)
                                    leg_time_hr = leg_dist / req.cruise_speed_kt if req.cruise_speed_kt > 0 else 0
                                    leg_fuel_gal = leg_time_hr * req.fuel_burn_gph
                                    leg_eval = helicopter_db.evaluate_leg(
                                        heli.type_code,
                                        dep_elev_ft=from_ap.elevation_ft,
                                        arr_elev_ft=to_ap.elevation_ft,
                                        max_enroute_elev_ft=max_terr + req.min_agl_ft,
                                        oat_c=req.oat_c,
                                        gross_weight_lb=current_weight if current_weight > 0 else None,
                                        fuel_burn_gal=leg_fuel_gal,
                                    )
                                    event["helicopter_perf"] = leg_eval
                                    # Update running weight: subtract fuel burned this leg
                                    current_weight -= leg_fuel_gal * fw_per_gal
                                    # Refuel at destination unless it's NF or final destination
                                    is_final_dest = (to_ap.icao == arr.icao and si == len(segment_endpoints) - 2 and i == seg_num_legs - 1)
                                    can_refuel = waypoint_fuel.get(to_ap.icao, True) and not is_final_dest
                                    if can_refuel:
                                        current_weight = initial_weight  # topped off → back to departure weight
                            if event.get("type") == "no_path":
                                blocked_pairs.add((from_ap.icao, to_ap.icao))
                                leg_failed = True
                                last_fail_event = event
                                last_fail_event["from"] = from_ap.icao
                                last_fail_event["to"] = to_ap.icao
                                last_fail_event["leg_index"] = global_leg
                                break
                            yield f"data: {json.dumps(event)}\n\n"

                        if leg_failed:
                            break

                    if not leg_failed:
                        all_sequences.append(sequence)
                        segment_ok = True
                        seq_succeeded = True
                        break
                    # else: try next candidate sequence from the pool

                if seq_succeeded:
                    break  # segment done, move to next

                # All candidate sequences failed for this attempt — reroute or give up
                if attempt < max_retries:
                    yield f"data: {json.dumps({'type': 'reroute', 'message': f'Attempt {attempt+1} failed — replanning segment with wider detour...', 'blocked': [list(p) for p in blocked_pairs], 'keep_legs': sum(len(s) - 1 for s in all_sequences)})}\n\n"
                else:
                    # Final failure — include diagnostic detail
                    msg = f"No terrain-avoiding path for leg: {last_fail_event['from']} → {last_fail_event['to']} (all {max_retries+1} attempts exhausted)"
                    detail = last_fail_event.get("detail", "")
                    if detail:
                        msg += f"\n{detail}"
                    last_fail_event["message"] = msg
                    yield f"data: {json.dumps(last_fail_event)}\n\n"
                    yield f"data: {json.dumps({'type': 'done'})}\n\n"
                    return

            if not segment_ok:
                return  # already yielded no_path/done above

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


class ProfileRequest(BaseModel):
    path: List[List[float]]  # [[lat, lon], ...]
    max_msl_ft: float = Field(gt=0)
    min_agl_ft: float = Field(ge=0)
    avoid_airspace: List[str] = []


_ALL_DISPLAY_CLASSES = ["B", "C", "D", "R", "P", "W", "MOA", "A", "DA"]


def _resample_profile_path(path: list[list[float]], max_step_nm: float = 0.25) -> list[list[float]]:
    """Densify short route segments so profile water/obstacle sampling is stable."""
    if len(path) <= 1:
        return list(path)

    result = [path[0]]
    for idx in range(len(path) - 1):
        lat0, lon0 = path[idx]
        lat1, lon1 = path[idx + 1]
        seg_nm = _haversine_nm(lat0, lon0, lat1, lon1)
        n_sub = max(1, int(math.ceil(seg_nm / max_step_nm)))
        for step in range(1, n_sub):
            t = step / n_sub
            result.append([
                lat0 + (lat1 - lat0) * t,
                lon0 + (lon1 - lon0) * t,
            ])
        result.append(path[idx + 1])
    return result

def _profile_airspace_zones(
    coords: list, elev_ft: list, avoid_classes: list[str], min_agl_ft: float,
) -> list[dict]:
    """Return airspace zones that overlap the path, with MSL floor/ceiling per sample point.

    Always queries ALL airspace classes for situational awareness display.
    """
    import json as _json
    from mvp_backend.planner import _point_in_polygon, _airspace_floor_msl, _airspace_ceiling_msl

    display_classes = _ALL_DISPLAY_CLASSES

    db_path = os.path.join(ROOT, "mvp_backend", "airspace_data", "airspace.sqlite")
    if not os.path.exists(db_path):
        return []

    lats = [c[0] for c in coords]
    lons = [c[1] for c in coords]
    bbox_min_lat, bbox_max_lat = min(lats), max(lats)
    bbox_min_lon, bbox_max_lon = min(lons), max(lons)

    ph = ",".join("?" for _ in display_classes)
    sql = f"""
        SELECT name, class, lower_alt, lower_code, upper_alt, upper_code, geometry
        FROM airspace
        WHERE class IN ({ph})
          AND max_lat >= ? AND min_lat <= ?
          AND max_lon >= ? AND min_lon <= ?
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            sql, display_classes + [bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon],
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    zones: list[dict] = []
    for name, cls, lower_alt, lower_code, upper_alt, upper_code, geom_str in rows:
        if lower_alt is None or upper_alt is None:
            continue
        geom = _json.loads(geom_str)
        gtype = geom.get("type", "")
        if gtype == "Polygon":
            rings = [geom["coordinates"][0]]
        elif gtype == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            continue

        # Test each sample point against this airspace polygon
        inside = [False] * len(coords)
        for ring in rings:
            rlons = [c[0] for c in ring]
            rlats = [c[1] for c in ring]
            rmin_lat, rmax_lat = min(rlats), max(rlats)
            rmin_lon, rmax_lon = min(rlons), max(rlons)
            for idx, (lat, lon) in enumerate(coords):
                if inside[idx]:
                    continue
                if lat < rmin_lat or lat > rmax_lat or lon < rmin_lon or lon > rmax_lon:
                    continue
                if _point_in_polygon(lon, lat, ring):
                    inside[idx] = True

        # Build contiguous index ranges where path is inside this airspace
        i = 0
        while i < len(inside):
            if not inside[i]:
                i += 1
                continue
            start = i
            while i < len(inside) and inside[i]:
                i += 1
            end = i - 1  # inclusive

            # Compute MSL floor/ceiling for each sample in [start, end]
            floors = []
            ceilings = []
            for idx in range(start, end + 1):
                terrain = elev_ft[idx]
                f = _airspace_floor_msl(lower_alt, lower_code, terrain)
                c = _airspace_ceiling_msl(upper_alt, upper_code, terrain)
                floors.append(round(f))
                ceilings.append(round(c))

            zones.append({
                "name": name or cls,
                "cls": cls,
                "start_idx": start,
                "end_idx": end,
                "floor_msl": floors,
                "ceiling_msl": ceilings,
            })

    return zones


@app.post("/elevation-profile")
def elevation_profile(req: ProfileRequest):
    """Return ground elevation profile along a route path."""
    from mvp_backend.terrain_provider import meters_to_feet

    if len(req.path) < 2:
        raise HTTPException(400, "Path must have at least 2 points")

    pts = _resample_profile_path(req.path)

    # Subsample if path has too many points (keep it fast)
    max_samples = 300
    if len(pts) > max_samples:
        step = len(pts) / max_samples
        pts = [pts[int(i * step)] for i in range(max_samples)]
        pts.append(req.path[-1])

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
    coords = [(p[0], p[1]) for p in pts]
    elev_m = provider.get_many_m(coords)
    elev_ft = [meters_to_feet(m) if m == m else 0.0 for m in elev_m]

    # Detect water at each profile sample.
    # Use a tolerant SRTM water signature rather than exact equality because
    # inland lakes often contain a mix of lake-surface and shoreline samples.
    _OFFSET = 0.001  # ~111 m
    water_probe_pts = []
    for lat, lon in coords:
        for dlat, dlon in ((-_OFFSET, 0), (_OFFSET, 0), (0, -_OFFSET), (0, _OFFSET)):
            water_probe_pts.append((lat + dlat, lon + dlon))
    probe_elev = provider.get_many_m(water_probe_pts)
    is_water = []
    _MATCH_TOL_M = 5.0
    _RANGE_TOL_M = 15.0
    for idx, (lat, lon) in enumerate(coords):
        e = elev_m[idx]
        if e != e:  # NaN → void → ocean
            is_water.append(True)
            continue
        if e <= 0:
            is_water.append(True)
            continue
        base = idx * 4
        neighbours = [probe_elev[base + k]
                      for k in range(4)
                      if probe_elev[base + k] == probe_elev[base + k]]  # skip NaN
        if len(neighbours) < 3:
            is_water.append(False)
            continue

        if all(abs(n - e) <= _MATCH_TOL_M for n in neighbours):
            is_water.append(True)
            continue

        all_elevs = [e, *neighbours]
        if max(all_elevs) - min(all_elevs) <= _RANGE_TOL_M:
            is_water.append(True)
        else:
            is_water.append(False)

    # Build cumulative distance
    dist_nm = [0.0]
    for i in range(1, len(coords)):
        d = _haversine_nm(coords[i-1][0], coords[i-1][1], coords[i][0], coords[i][1])
        dist_nm.append(dist_nm[-1] + d)

    # Compute airspace zones along the path
    airspace_zones = _profile_airspace_zones(
        coords, elev_ft, req.avoid_airspace, req.min_agl_ft,
    )

    return {
        "dist_nm": [round(d, 2) for d in dist_nm],
        "ground_ft": [round(e, 0) for e in elev_ft],
        "is_water": is_water,
        "max_msl_ft": req.max_msl_ft,
        "min_agl_ft": req.min_agl_ft,
        "airspace_zones": airspace_zones,
    }


@app.get("/elevation")
def elevation_at_point(lat: float = Query(...), lon: float = Query(...)):
    """Return ground elevation (ft MSL) at a single lat/lon point."""
    from mvp_backend.terrain_provider import meters_to_feet
    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))
    elev_m = provider.get_many_m([(lat, lon)])[0]
    if elev_m != elev_m:  # NaN
        return {"lat": lat, "lon": lon, "elev_ft": None}
    return {"lat": lat, "lon": lon, "elev_ft": round(meters_to_feet(elev_m), 0)}


# ── cache stats endpoint ─────────────────────────────────────────────

@app.get("/cache/stats")
def cache_stats():
    """Return cache and route history statistics."""
    return route_cache.cache_stats()


@app.post("/cache/clear")
def cache_clear():
    """Clear all cached route legs."""
    return route_cache.clear_cache()


# ── terrain intelligence endpoints ───────────────────────────────────

@app.get("/terrain-intel/summary")
def terrain_intel_summary():
    """Return terrain intelligence coverage summary."""
    return terrain_intel.coverage_summary()


@app.get("/terrain-intel/check")
def terrain_intel_check(
    from_lat: float = Query(...), from_lon: float = Query(...),
    to_lat: float = Query(...), to_lon: float = Query(...),
    msl_ft: float = Query(5000), agl_ft: float = Query(1000),
):
    """Quick viability check for a corridor."""
    return terrain_intel.check_viability(from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft)


@app.post("/terrain-intel/precompute")
def terrain_intel_precompute(
    lat_min: int = Query(31), lat_max: int = Query(49),
    lon_min: int = Query(-125), lon_max: int = Query(-104),
):
    """Trigger background terrain precomputation."""
    def _run():
        terrain_intel.precompute_region(
            lat_range=(lat_min, lat_max),
            lon_range=(lon_min, lon_max),
        )
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return {"status": "started", "region": {
        "lat": [lat_min, lat_max], "lon": [lon_min, lon_max]
    }}


# ── obstacles endpoint ───────────────────────────────────────────────

_OBS_DB = os.path.join(os.path.dirname(__file__), "obstacle_data", "obstacles.sqlite")

@app.get("/obstacles")
def get_obstacles(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
    min_agl: int = Query(200),
):
    """Return obstacles within a bounding box, filtered by min AGL height."""
    if not os.path.exists(_OBS_DB):
        return []
    conn = sqlite3.connect(_OBS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT lat, lon, agl, amsl, type, lit FROM obstacles "
        "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? AND agl >= ? "
        "LIMIT 5000",
        (south, north, west, east, min_agl),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── safe-landing-areas endpoint (OSM-derived, slope-filtered) ────────

@app.get("/landing-areas")
def get_landing_areas(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
    max_slope_deg: float = Query(8.0, ge=1.0, le=45.0),
):
    """Return landable OSM polygons (farmland / meadow / grass /
    grassland / golf / park / aerodrome / airstrip / helipad) inside the
    bbox, filtered by terrain slope sampled at each polygon centroid.
    Cached aggressively so repeat panning is free.
    """
    return _landing_areas.query_bbox(south, north, west, east,
                                     max_slope_deg=max_slope_deg)


@app.get("/landcover")
def get_landcover(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
):
    """Return categorised OSM landcover features in bbox: roads, forest,
    urban, open. Used both as a map overlay and as a planner cost input
    when use_landcover=True is sent on /route/stream."""
    return _landcover.query_bbox(south, north, west, east)


@app.get("/tfrs")
def get_tfrs(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
):
    """Return active FAA TFRs intersecting the bbox.

    Refreshes from the FAA TFR feed every ~15 min. If the feed is down,
    serves the last cached snapshot (which may be empty on a cold cache).
    Wrapped in try/except — TFR data must NEVER take down the API.
    """
    try:
        return _tfrs.get_active_tfrs(bbox=(south, west, north, east))
    except Exception:
        return []


# ── airspace endpoint ────────────────────────────────────────────────

_AIRSPACE_DB = os.path.join(os.path.dirname(__file__), "airspace_data", "airspace.sqlite")


def _airports_in_airspace(airports_list, classes=("B", "C")):
    """Return list of {icao, airspace_class, airspace_name} for airports
    that fall inside any of the given airspace classes."""
    if not os.path.exists(_AIRSPACE_DB) or not airports_list:
        return []
    placeholders = ",".join("?" for _ in classes)
    conn = sqlite3.connect(_AIRSPACE_DB)
    results = []
    for ap in airports_list:
        rows = conn.execute(
            f"SELECT class, name, geometry FROM airspace "
            f"WHERE class IN ({placeholders}) "
            f"AND min_lat <= ? AND max_lat >= ? AND min_lon <= ? AND max_lon >= ?",
            list(classes) + [ap.lat, ap.lat, ap.lon, ap.lon]
        ).fetchall()
        for cls, name, geom_str in rows:
            import json as _json
            geom = _json.loads(geom_str)
            gtype = geom.get("type", "")
            if gtype == "Polygon":
                rings = [geom["coordinates"][0]]
            elif gtype == "MultiPolygon":
                rings = [poly[0] for poly in geom["coordinates"]]
            else:
                continue
            for ring in rings:
                if _point_in_ring(ap.lon, ap.lat, ring):
                    results.append({"icao": ap.icao, "airspace_class": cls, "airspace_name": name})
                    break
            else:
                continue
            break  # found one match for this airport, skip remaining polygons
    conn.close()
    return results


def _point_in_ring(px, py, ring):
    """Ray-casting point-in-polygon."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > py) != (yj > py)) and (px < (xj - xi) * (py - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside

@app.get("/airspace")
def get_airspace(
    south: float = Query(...), north: float = Query(...),
    west: float = Query(...), east: float = Query(...),
    classes: str = Query("B,C,D,R,P,W,MOA,A"),
):
    """Return airspace polygons that intersect a bounding box."""
    if not os.path.exists(_AIRSPACE_DB):
        return []
    cls_list = [c.strip().upper() for c in classes.split(",") if c.strip()]
    if not cls_list:
        return []
    conn = sqlite3.connect(_AIRSPACE_DB)
    conn.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in cls_list)
    rows = conn.execute(
        f"SELECT ident, icao_id, name, class, local_type, "
        f"lower_alt, lower_code, upper_alt, upper_code, times_of_use, geometry "
        f"FROM airspace "
        f"WHERE max_lat >= ? AND min_lat <= ? AND max_lon >= ? AND min_lon <= ? "
        f"AND class IN ({placeholders}) "
        f"LIMIT 2000",
        (south, north, west, east, *cls_list),
    ).fetchall()
    conn.close()
    import json as _json
    result = []
    for r in rows:
        d = dict(r)
        d["geometry"] = _json.loads(d["geometry"])
        result.append(d)
    return result


# ── adventure mode ─────────────────────────────────────────────────────
from mvp_backend import adventure as _adventure
from mvp_backend import poi_provider as _poi_provider


class AdventureSearchRequest(BaseModel):
    origin: str = Field(..., description="ICAO code or @lat,lon")
    time_aloft_min: float = Field(60.0, gt=0, le=600,
                                  description="Total time aloft budget (min). If round_trip, halved for one-way leg.")
    vibes: list[str] = Field(default_factory=list,
                             description="Subset of: hike, camp, fish, ski, hot_springs, scenic, food. Empty = all.")
    helicopter_type: str = Field("", description="Heli type code; used to derive cruise speed if cruise_kt not given.")
    cruise_kt: float = Field(0.0, ge=0)
    round_trip: bool = Field(True)
    limit: int = Field(25, gt=0, le=200)
    auto_fetch: bool = Field(True, description="If true, fetch missing POI cells from Overpass.")
    # Performance inputs
    oat_c: float = Field(15.0, description="OAT °C at origin for density-altitude perf.")
    gross_weight_lb: float = Field(0.0, ge=0, description="Gross weight; 0 = use heli max GW.")
    usable_fuel_gal: float = Field(0.0, ge=0, description="Usable fuel; 0 = heli default.")
    fuel_burn_gph: float = Field(0.0, ge=0, description="Burn override; 0 = derive from heli perf at DA.")
    reserve_min: float = Field(30.0, ge=0, description="Fuel reserve in minutes.")


@app.post("/adventure/search")
def adventure_search(req: AdventureSearchRequest):
    return _adventure.search(_adventure.AdventureRequest(
        origin=req.origin,
        time_aloft_min=req.time_aloft_min,
        vibes=req.vibes or None,
        helicopter_type=req.helicopter_type,
        cruise_kt=req.cruise_kt,
        round_trip=req.round_trip,
        limit=req.limit,
        auto_fetch=req.auto_fetch,
        oat_c=req.oat_c,
        gross_weight_lb=req.gross_weight_lb,
        usable_fuel_gal=req.usable_fuel_gal,
        fuel_burn_gph=req.fuel_burn_gph,
        reserve_min=req.reserve_min,
    ))


@app.get("/adventure/vibes")
def adventure_vibes():
    """List supported vibe categories."""
    return {"vibes": list(_poi_provider.VALID_VIBES)}


@app.get("/adventure/cache/stats")
def adventure_cache_stats():
    return _poi_provider.stats()


class FavoriteRequest(BaseModel):
    user_key: str = Field(..., min_length=1, max_length=128,
                          description="Opaque per-device id (frontend stores in localStorage).")
    poi_id: str
    payload: dict = Field(default_factory=dict)


@app.get("/adventure/favorites")
def favorites_list(user_key: str = Query(..., min_length=1, max_length=128)):
    return _adventure.list_favorites(user_key)


@app.post("/adventure/favorites/add")
def favorites_add(req: FavoriteRequest):
    return _adventure.add_favorite(req.user_key, req.poi_id, req.payload)


@app.post("/adventure/favorites/remove")
def favorites_remove(req: FavoriteRequest):
    return _adventure.remove_favorite(req.user_key, req.poi_id)


# ── background precomputation ─────────────────────────────────────────

_bg_thread: threading.Thread | None = None
_bg_stop = threading.Event()


def _background_precompute():
    """Precompute A* legs for popular routes that aren't yet cached."""
    while not _bg_stop.is_set():
        _bg_stop.wait(60)  # run every 60 seconds
        if _bg_stop.is_set():
            break
        try:
            popular = route_cache.popular_routes(limit=10)
            if not popular:
                continue
            airports = load_airports_solver()
            for entry in popular:
                if _bg_stop.is_set():
                    break
                dep = airports.get(entry["dep"])
                arr = airports.get(entry["arr"])
                if not dep or not arr:
                    continue
                # plan fuel stop sequence to get the individual legs
                seq = plan_stop_sequence(
                    dep=dep, arr=arr, airports=airports,
                    cruise_speed_kt=entry.get("cruise_kt", 120),
                    usable_fuel_gal=entry.get("fuel_gal", 48),
                    burn_gph=entry.get("burn_gph", 13),
                    reserve_min=entry.get("reserve_min", 30),
                    required_fuel=entry.get("fuel_type", "100LL"),
                    max_detour_factor=entry["detour_fac"],
                    max_msl_ft=entry["max_msl_ft"],
                    min_agl_ft=entry["min_agl_ft"],
                    max_climb_fpm=entry.get("max_climb_fpm", 0),
                    max_descent_fpm=entry.get("max_descent_fpm", 0),
                    climb_speed_kt=entry.get("climb_speed_kt", 0),
                    descent_speed_kt=entry.get("descent_speed_kt", 0),
                )
                if not seq:
                    continue
                # precompute each leg (cache lookup inside will skip already-cached)
                for i in range(len(seq) - 1):
                    if _bg_stop.is_set():
                        break
                    terrain_avoid_leg(
                        seq[i], seq[i + 1],
                        max_msl_ft=entry["max_msl_ft"],
                        min_agl_ft=entry["min_agl_ft"],
                        max_detour_factor=entry["detour_fac"],
                        max_climb_fpm=entry.get("max_climb_fpm", 0),
                        max_descent_fpm=entry.get("max_descent_fpm", 0),
                        cruise_speed_kt=entry.get("cruise_kt", 120),
                        climb_speed_kt=entry.get("climb_speed_kt", 0),
                        descent_speed_kt=entry.get("descent_speed_kt", 0),
                    )
        except Exception:
            pass  # background worker should never crash the app


@app.on_event("startup")
def _start_bg_worker():
    global _bg_thread
    _bg_thread = threading.Thread(target=_background_precompute, daemon=True)
    _bg_thread.start()


@app.on_event("shutdown")
def _stop_bg_worker():
    _bg_stop.set()
    if _bg_thread:
        _bg_thread.join(timeout=5)
