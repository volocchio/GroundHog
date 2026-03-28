from __future__ import annotations

import csv
import json
import math
import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from mvp_backend.grid_astar import GridSpec, astar_path, astar_path_streaming, path_nm
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend import route_cache


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_AIRPORTS_SOLVER = os.path.join(ROOT, "mvp_backend", "airports_solver.csv")
CACHE_DB = os.path.join(ROOT, "mvp_backend", "elev_cache.sqlite")
_AIRSPACE_DB = os.path.join(os.path.dirname(__file__), "airspace_data", "airspace.sqlite")
_OBSTACLE_DB = os.path.join(os.path.dirname(__file__), "obstacle_data", "obstacles.sqlite")


@dataclass
class Airport:
    icao: str
    name: str
    lat: float
    lon: float
    elevation_ft: float
    facility_use: str
    fuel_100ll: int
    fuel_jeta: int


def load_airports_solver(path: str = DATA_AIRPORTS_SOLVER) -> Dict[str, Airport]:
    out: Dict[str, Airport] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            icao = (row.get("icao") or "").strip().upper()
            lid = (row.get("lid") or "").strip().upper()
            if not icao and not lid:
                continue
            ap = Airport(
                icao=icao or lid,
                name=row.get("name") or "",
                lat=float(row.get("lat") or 0),
                lon=float(row.get("lon") or 0),
                elevation_ft=float(row.get("elevation_ft") or 0),
                facility_use=(row.get("facility_use") or "").strip().upper(),
                fuel_100ll=int(row.get("fuel_100ll") or 0),
                fuel_jeta=int(row.get("fuel_jeta") or 0),
            )
            # Index by ICAO code
            if icao:
                out[icao] = ap
            # Also index by LID so users can type either
            if lid and lid not in out:
                out[lid] = ap
    return out


def _deg_per_km_lat() -> float:
    return 1.0 / 110.574


def _deg_per_km_lon(lat: float) -> float:
    return 1.0 / (111.320 * math.cos(math.radians(lat)) + 1e-9)


def _direct_nm(a: Airport, b: Airport) -> float:
    # reuse haversine from grid_astar
    from mvp_backend.grid_astar import _haversine_nm
    return _haversine_nm(a.lat, a.lon, b.lat, b.lon)


@dataclass
class LegResult:
    dist_nm: float
    path_latlon: List[Tuple[float, float]]


def terrain_avoid_leg(
    a: Airport,
    b: Airport,
    max_msl_ft: float,
    min_agl_ft: float,
    max_detour_factor: float,
    cell_km: float = 1.0,
    initial_margin_km: float = 40.0,
    margin_step_km: float = 40.0,
    max_margin_km: float = 400.0,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_speed_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
) -> Optional[LegResult]:
    """Find terrain-avoiding path between airports using grid A*.

    Expands a bounding-box margin until a path is found with acceptable detour.
    When max_climb_fpm/max_descent_fpm > 0, also enforces climb/descent rate
    limits per grid edge (terrain-following constraint).
    """
    # ── persistent cache lookup ──
    cached = route_cache.get_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor,
                                 max_climb_fpm, max_descent_fpm,
                                 climb_speed_kt, descent_speed_kt)
    if cached is not None:
        if cached.dist_nm == float("inf"):
            return None
        return LegResult(dist_nm=cached.dist_nm, path_latlon=cached.path)

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))

    direct_nm = _direct_nm(a, b)
    direct_km = direct_nm * 1.852
    detour_limit_nm = max_detour_factor * direct_nm

    ceiling_ft = max_msl_ft - min_agl_ft

    mid_lat = 0.5 * (a.lat + b.lat)
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    # Scale initial margin so the grid captures alternate routes around
    # large obstacles (e.g. mountain ranges perpendicular to the track).
    margin_km = max(initial_margin_km, direct_km * 0.55)
    while margin_km <= max_margin_km:
        # bbox expanded
        min_lat = min(a.lat, b.lat) - margin_km * _deg_per_km_lat()
        max_lat = max(a.lat, b.lat) + margin_km * _deg_per_km_lat()
        min_lon = min(a.lon, b.lon) - margin_km * _deg_per_km_lon(mid_lat)
        max_lon = max(a.lon, b.lon) + margin_km * _deg_per_km_lon(mid_lat)

        n_lat = int(math.ceil((max_lat - min_lat) / dlat)) + 1
        n_lon = int(math.ceil((max_lon - min_lon) / dlon)) + 1

        # Adaptive cell coarsening: if grid too large, increase cell size
        _MAX_CELLS = 500_000
        _dlat, _dlon = dlat, dlon
        while n_lat * n_lon > _MAX_CELLS:
            _dlat *= 1.5
            _dlon *= 1.5
            n_lat = int(math.ceil((max_lat - min_lat) / _dlat)) + 1
            n_lon = int(math.ceil((max_lon - min_lon) / _dlon)) + 1

        grid = GridSpec(lat0=min_lat, lon0=min_lon, n_lat=n_lat, n_lon=n_lon, dlat=_dlat, dlon=_dlon)

        # generate points in row-major order for batch elevation
        points = [grid.idx_to_latlon(i, j) for i in range(n_lat) for j in range(n_lon)]
        elev_m = provider.get_many_m(points)
        elev_ft = [meters_to_feet(m) if m == m else float("inf") for m in elev_m]

        passable: List[List[bool]] = [[True] * n_lon for _ in range(n_lat)]
        elev_ft_2d: Optional[List[List[float]]] = None
        if max_climb_fpm > 0 or max_descent_fpm > 0:
            elev_ft_2d = [[0.0] * n_lon for _ in range(n_lat)]
        k = 0
        for i in range(n_lat):
            row = passable[i]
            for j in range(n_lon):
                row[j] = elev_ft[k] <= ceiling_ft
                if elev_ft_2d is not None:
                    elev_ft_2d[i][j] = elev_ft[k]
                k += 1

        start = grid.latlon_to_idx(a.lat, a.lon)
        goal = grid.latlon_to_idx(b.lat, b.lon)
        if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
            # start/goal blocked at this ceiling
            margin_km += margin_step_km
            continue

        path_idx = astar_path(grid, passable, start, goal,
                              elev_ft=elev_ft_2d,
                              max_climb_fpm=max_climb_fpm,
                              max_descent_fpm=max_descent_fpm,
                              cruise_kt=cruise_speed_kt,
                              climb_speed_kt=climb_speed_kt,
                              descent_speed_kt=descent_speed_kt)
        if not path_idx:
            margin_km += margin_step_km
            continue

        dist_nm = path_nm(grid, path_idx)
        if dist_nm <= detour_limit_nm:
            path_latlon = [grid.idx_to_latlon(i, j) for i, j in path_idx]
            route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor, dist_nm, path_latlon,
                                max_climb_fpm, max_descent_fpm,
                                climb_speed_kt, descent_speed_kt)
            return LegResult(dist_nm=dist_nm, path_latlon=path_latlon)

        # Found path but too long: expand margin (sometimes finds a shorter corridor)
        margin_km += margin_step_km

    route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor, None, None,
                        max_climb_fpm, max_descent_fpm,
                        climb_speed_kt, descent_speed_kt)
    return None


def leg_fuel_ok(
    dist_nm: float,
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    *,
    dep_elev_ft: float | None = None,
    arr_elev_ft: float | None = None,
    cruise_alt_ft: float | None = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
) -> tuple[bool, float, float]:
    """Fuel feasibility for a leg.

    MVP model:
    - Base enroute time = dist / cruise_speed.
    - If climb/descent rates and speeds are provided, add climb/descent *time*
      based on a simple climb to `cruise_alt_ft` above each airport elevation.

    This is intentionally approximate (but better than ignoring climb/descent
    when users provide those fields).
    """

    cruise_speed = max(1e-6, cruise_speed_kt)
    time_hr = dist_nm / cruise_speed

    # Optional climb/descent time model (used even when not terrain-following)
    if (
        dep_elev_ft is not None
        and arr_elev_ft is not None
        and cruise_alt_ft is not None
        and cruise_alt_ft > 0
        and (max_climb_fpm > 0 or max_descent_fpm > 0)
    ):
        # Clamp target altitude above each field elevation
        climb_ft = max(0.0, cruise_alt_ft - dep_elev_ft)
        descent_ft = max(0.0, cruise_alt_ft - arr_elev_ft)

        climb_time_hr = (climb_ft / max(1e-6, max_climb_fpm)) / 60.0 if max_climb_fpm > 0 else 0.0
        descent_time_hr = (descent_ft / max(1e-6, max_descent_fpm)) / 60.0 if max_descent_fpm > 0 else 0.0

        # Account for climb/descent speeds by allocating some of the leg's
        # distance to climb/descent segments.
        cs = climb_speed_kt if climb_speed_kt and climb_speed_kt > 0 else cruise_speed
        ds = descent_speed_kt if descent_speed_kt and descent_speed_kt > 0 else cruise_speed

        climb_nm = climb_time_hr * cs
        descent_nm = descent_time_hr * ds
        cruise_nm = max(0.0, dist_nm - climb_nm - descent_nm)

        time_hr = (cruise_nm / cruise_speed) + climb_time_hr + descent_time_hr

    reserve_gal = (reserve_min / 60.0) * burn_gph
    needed_gal = time_hr * burn_gph + reserve_gal
    return (needed_gal <= usable_fuel_gal, time_hr, needed_gal)


def limiting_point_along_path(
    path_latlon: List[Tuple[float, float]],
    min_agl_ft: float,
    provider: Optional[SRTMProvider] = None,
    sample_step: int = 5,
) -> Optional[dict]:
    """Compute the limiting terrain point along a polyline.

    Returns dict with max_terrain_ft, required_msl_ft, lat, lon.

    `sample_step` subsamples the path to keep it fast.
    """
    if not path_latlon:
        return None

    if provider is None:
        provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))

    pts = path_latlon[:: max(1, sample_step)]
    if pts[-1] != path_latlon[-1]:
        pts.append(path_latlon[-1])

    elev_m = provider.get_many_m(pts)
    best = None
    for (lat, lon), em in zip(pts, elev_m):
        if em != em:
            continue
        terrain_ft = meters_to_feet(em)
        req_msl = terrain_ft + min_agl_ft
        if best is None or req_msl > best["required_msl_ft"]:
            best = {
                "lat": lat,
                "lon": lon,
                "terrain_ft": float(terrain_ft),
                "required_msl_ft": float(req_msl),
            }
    return best


def plan_stop_sequences(
    dep: Airport,
    arr: Airport,
    airports: Dict[str, Airport],
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    required_fuel: str,
    max_detour_factor: float,
    max_expansions: int = 2000,
    max_neighbors: int = 120,
    blocked_pairs: Optional[set] = None,
    max_msl_ft: float = 0,
    min_agl_ft: float = 0,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    start_fuel_gal: float = 0,
    k: int = 3,
) -> List[List[Airport]]:
    """Return up to *k* diverse fuel-stop sequences, best first.

    Each sequence is [dep, stop1, ..., arr].  Returns empty list on total
    failure (reserve exceeds fuel, zero candidates, etc.).

    Uses Yen's K-shortest-paths over a fuel-stop A* graph so the server
    can try alternative stop choices when the first sequence has a
    terrain-blocked leg.
    """
    import heapq

    _blocked = blocked_pairs or set()

    def _is_blocked(from_code: str, to_code: str) -> bool:
        if (from_code, to_code) in _blocked:
            return True
        if max_msl_ft > 0 and route_cache.is_known_failure(
                from_code, to_code, max_msl_ft, min_agl_ft, max_detour_factor,
                max_climb_fpm, max_descent_fpm,
                climb_speed_kt, descent_speed_kt):
            return True
        return False

    # Realistic planning cushion — 15 % accounts for typical terrain detours
    # without forcing unnecessary fuel stops.
    planning_detour = min(max_detour_factor, 1.15)

    stop_penalty_hr = 0.5

    _start_fuel = start_fuel_gal if start_fuel_gal > 0 else usable_fuel_gal

    # Direct route shortcut
    direct_nm = _direct_nm(dep, arr)
    ok, _, _ = leg_fuel_ok(direct_nm * planning_detour, cruise_speed_kt,
                           _start_fuel, burn_gph, reserve_min)
    if ok and not _is_blocked(dep.icao, arr.icao):
        return [[dep, arr]]

    want_100ll = required_fuel.upper() == "100LL"

    max_leg_time = (usable_fuel_gal / burn_gph) - (reserve_min / 60.0)
    if max_leg_time <= 0:
        return []
    max_leg_nm = max_leg_time * cruise_speed_kt

    start_leg_time = (_start_fuel / burn_gph) - (reserve_min / 60.0)
    start_max_leg_nm = start_leg_time * cruise_speed_kt if start_leg_time > 0 else 0

    # Elevation ceiling for candidate filtering
    ceiling_ft = (max_msl_ft - min_agl_ft) if max_msl_ft > 0 else 0

    # Build candidate fuel stops — skip unreachable-altitude airports
    stop_candidates: List[Airport] = []
    for ap in airports.values():
        if ap.facility_use != "PU":
            continue
        if want_100ll and ap.fuel_100ll != 1:
            continue
        if (not want_100ll) and ap.fuel_jeta != 1:
            continue
        if ceiling_ft > 0 and ap.elevation_ft > ceiling_ft:
            continue  # airport above our ceiling — plane can't land
        stop_candidates.append(ap)

    def get_airport(code: str) -> Airport:
        if code == dep.icao:
            return dep
        if code == arr.icao:
            return arr
        return airports[code]

    # ── Core A* returning (best_cost, prev) so we can reconstruct paths ──
    def _run_astar(edge_penalty: Optional[Dict[tuple, float]] = None):
        best_cost: Dict[str, float] = {dep.icao: 0.0}
        prev_map: Dict[str, str] = {}
        h_dep = _direct_nm(dep, arr) / cruise_speed_kt if cruise_speed_kt > 0 else 0
        pq: list[tuple[float, str]] = [(h_dep, dep.icao)]
        expanded = 0
        radius = max_leg_nm
        total_direct = _direct_nm(dep, arr) or 1.0

        while pq and expanded < max_expansions:
            f_cost, cur_code = heapq.heappop(pq)
            cur_g = best_cost.get(cur_code, float("inf"))
            if cur_code != dep.icao:
                h_cur = _direct_nm(get_airport(cur_code), arr) / cruise_speed_kt if cruise_speed_kt > 0 else 0
                if f_cost > cur_g + h_cur + 1e-9:
                    continue
            if cur_code == arr.icao:
                break

            expanded += 1
            cur_ap = get_airport(cur_code)
            cur_radius = start_max_leg_nm if cur_code == dep.icao else radius
            cur_fuel = _start_fuel if cur_code == dep.icao else usable_fuel_gal
            d_cur_to_arr = _direct_nm(cur_ap, arr)

            neigh: List[tuple[float, Airport]] = []
            if d_cur_to_arr <= cur_radius:
                neigh.append((d_cur_to_arr, arr))

            for ap in stop_candidates:
                if ap.icao == cur_code:
                    continue
                d = _direct_nm(cur_ap, ap)
                if d <= cur_radius:
                    neigh.append((d, ap))

            # Sort by distance-to-destination (forward progress) instead of
            # distance-from-current, so forward airports are explored first.
            neigh.sort(key=lambda x: _direct_nm(x[1], arr))
            neigh = neigh[:max_neighbors]

            for d, nxt_ap in neigh:
                if _is_blocked(cur_code, nxt_ap.icao):
                    continue
                ok, t_hr, _ = leg_fuel_ok(d * planning_detour, cruise_speed_kt,
                                          cur_fuel, burn_gph, reserve_min)
                if not ok:
                    continue
                penalty = 0.0 if nxt_ap.icao == arr.icao else stop_penalty_hr
                extra = 0.0
                if edge_penalty:
                    extra = edge_penalty.get((cur_code, nxt_ap.icao), 0.0)

                # Backtrack penalty: if this hop moves AWAY from the
                # destination, add a time cost proportional to how much
                # backward progress it represents.
                d_nxt_to_arr = _direct_nm(nxt_ap, arr)
                backtrack_nm = d_nxt_to_arr - d_cur_to_arr
                backtrack_penalty = 0.0
                if backtrack_nm > 0 and cruise_speed_kt > 0:
                    # Cost = twice the backward distance converted to hours
                    backtrack_penalty = (backtrack_nm * 2.0) / cruise_speed_kt

                new_g = cur_g + t_hr + penalty + extra + backtrack_penalty
                if new_g < best_cost.get(nxt_ap.icao, float("inf")):
                    best_cost[nxt_ap.icao] = new_g
                    prev_map[nxt_ap.icao] = cur_code
                    h = _direct_nm(nxt_ap, arr) / cruise_speed_kt if cruise_speed_kt > 0 else 0
                    heapq.heappush(pq, (new_g + h, nxt_ap.icao))

        return best_cost, prev_map

    def _reconstruct(prev_map: Dict[str, str]) -> Optional[List[Airport]]:
        if arr.icao not in prev_map and arr.icao != dep.icao:
            return None
        seq: List[Airport] = []
        cur = arr.icao
        while cur != dep.icao:
            seq.append(get_airport(cur))
            p = prev_map.get(cur)
            if not p:
                return None
            cur = p
        seq.append(dep)
        seq.reverse()
        return seq

    # ── First (best) sequence ──
    best_cost, prev_map = _run_astar()
    first = _reconstruct(prev_map)
    if first is None:
        return []

    results: List[List[Airport]] = [first]
    if k <= 1:
        return results

    # ── K-diverse sequences via edge-penalty variant of Yen's algorithm ──
    # Instead of full Yen's (expensive), we penalise edges used by earlier
    # solutions so A* is nudged toward different intermediate stops.
    edge_penalty: Dict[tuple, float] = {}
    penalty_inc = 2.0  # hours — large enough to force different routes
    for seq_idx in range(1, k):
        # Penalise edges from all previous solutions
        for prev_sol in results:
            for ei in range(len(prev_sol) - 1):
                key = (prev_sol[ei].icao, prev_sol[ei + 1].icao)
                edge_penalty[key] = edge_penalty.get(key, 0.0) + penalty_inc

        _, pm = _run_astar(edge_penalty=edge_penalty)
        nxt = _reconstruct(pm)
        if nxt is None:
            break
        # Deduplicate: skip if same stop list as an earlier result
        nxt_codes = [ap.icao for ap in nxt]
        if any(nxt_codes == [ap.icao for ap in r] for r in results):
            break
        results.append(nxt)

    return results


def plan_stop_sequence(
    dep: Airport,
    arr: Airport,
    airports: Dict[str, Airport],
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    required_fuel: str,
    max_detour_factor: float,
    max_expansions: int = 2000,
    max_neighbors: int = 120,
    blocked_pairs: Optional[set] = None,
    max_msl_ft: float = 0,
    min_agl_ft: float = 0,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    start_fuel_gal: float = 0,
) -> Optional[List[Airport]]:
    """Convenience wrapper: returns the single best stop sequence or None."""
    seqs = plan_stop_sequences(
        dep, arr, airports, cruise_speed_kt, usable_fuel_gal, burn_gph,
        reserve_min, required_fuel, max_detour_factor, max_expansions,
        max_neighbors, blocked_pairs, max_msl_ft, min_agl_ft,
        max_climb_fpm, max_descent_fpm, climb_speed_kt, descent_speed_kt,
        start_fuel_gal, k=1,
    )
    return seqs[0] if seqs else None


def plan_route_multi_stop(
    dep: Airport,
    arr: Airport,
    airports: Dict[str, Airport],
    cruise_speed_kt: float,
    usable_fuel_gal: float,
    burn_gph: float,
    reserve_min: float,
    max_msl_ft: float,
    min_agl_ft: float,
    required_fuel: str,
    max_detour_factor: float,
    max_expansions: int = 400,
    max_neighbors: int = 60,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
) -> dict:
    """Multi-stop route planner (unbounded stops) minimizing total time.

    Implementation: Dijkstra over airports.
    - Nodes: dep, arr, and public-use fuel airports (for intermediate stops)
    - Edge feasibility: terrain_avoid_leg + fuel constraint

    Notes:
    - This is MVP-grade: we aggressively prune neighbor candidates by straight-line range
      and only expand up to max_expansions nodes to keep runtime bounded.
    """

    want_100ll = required_fuel.upper() == "100LL"

    # Max leg range based on fuel after reserve
    max_leg_time = (usable_fuel_gal / burn_gph) - (reserve_min / 60.0)
    if max_leg_time <= 0:
        return {
            "type": "no_route",
            "message": "No route found: reserve exceeds usable fuel.",
            "suggestions": ["Decrease reserve", "Increase usable fuel"],
        }
    max_leg_nm = max_leg_time * cruise_speed_kt

    # Build candidate stop list once
    stop_candidates: List[Airport] = []
    for ap in airports.values():
        if ap.facility_use != "PU":
            continue
        if want_100ll and ap.fuel_100ll != 1:
            continue
        if (not want_100ll) and ap.fuel_jeta != 1:
            continue
        stop_candidates.append(ap)

    # include destination as possible neighbor even if it lacks fuel
    # origin is always allowed

    import heapq

    # state keyed by airport ICAO
    best_time: Dict[str, float] = {dep.icao: 0.0}
    prev: Dict[str, tuple[str, dict]] = {}

    # cache terrain legs to avoid recomputation
    leg_cache: Dict[tuple[str, str], tuple[float, float, float, List[tuple[float, float]]]] = {}
    # (from,to) -> (dist_nm, time_hr, fuel_gal, path)

    pq: list[tuple[float, str]] = [(0.0, dep.icao)]
    expanded = 0

    def get_airport(code: str) -> Airport:
        if code == dep.icao:
            return dep
        if code == arr.icao:
            return arr
        return airports[code]

    while pq and expanded < max_expansions:
        cur_t, cur_code = heapq.heappop(pq)
        if cur_t != best_time.get(cur_code, float("inf")):
            continue
        if cur_code == arr.icao:
            break

        expanded += 1
        cur_ap = get_airport(cur_code)

        # Candidate neighbors: fuel stops + destination
        # Prune by straight-line range (max_leg_nm is already the fuel range)
        radius_nm = max_leg_nm
        d_cur_to_arr = _direct_nm(cur_ap, arr)

        neigh: List[tuple[float, Airport]] = []
        # destination
        if d_cur_to_arr <= radius_nm:
            neigh.append((d_cur_to_arr, arr))

        for ap in stop_candidates:
            if ap.icao == cur_code:
                continue
            d = _direct_nm(cur_ap, ap)
            if d <= radius_nm:
                neigh.append((d, ap))

        # Sort by distance-to-destination so forward-progress airports are
        # explored first, preventing unnecessary backtracking.
        neigh.sort(key=lambda x: _direct_nm(x[1], arr))
        neigh = neigh[:max_neighbors]

        for _, nxt_ap in neigh:
            key = (cur_code, nxt_ap.icao)
            cached = leg_cache.get(key)
            if cached is None:
                leg = terrain_avoid_leg(cur_ap, nxt_ap, max_msl_ft, min_agl_ft, max_detour_factor,
                                        max_climb_fpm=max_climb_fpm, max_descent_fpm=max_descent_fpm,
                                        cruise_speed_kt=cruise_speed_kt,
                                        climb_speed_kt=climb_speed_kt,
                                        descent_speed_kt=descent_speed_kt)
                if not leg:
                    leg_cache[key] = (float("inf"), float("inf"), float("inf"), [])
                    continue
                ok, t_hr, gal = leg_fuel_ok(leg.dist_nm, cruise_speed_kt, usable_fuel_gal, burn_gph, reserve_min)
                if not ok:
                    leg_cache[key] = (float("inf"), float("inf"), float("inf"), [])
                    continue
                cached = (leg.dist_nm, t_hr, gal, leg.path_latlon)
                leg_cache[key] = cached

            dist_nm, t_hr, gal, path = cached
            if not math.isfinite(t_hr):
                continue

            # Backtrack penalty: penalize hops that move away from destination
            d_nxt_to_arr = _direct_nm(nxt_ap, arr)
            backtrack_nm = d_nxt_to_arr - d_cur_to_arr
            backtrack_penalty = 0.0
            if backtrack_nm > 0 and cruise_speed_kt > 0:
                backtrack_penalty = (backtrack_nm * 2.0) / cruise_speed_kt

            new_t = cur_t + t_hr + backtrack_penalty
            if new_t < best_time.get(nxt_ap.icao, float("inf")):
                best_time[nxt_ap.icao] = new_t
                prev[nxt_ap.icao] = (cur_code, {"from": cur_code, "to": nxt_ap.icao, "dist_nm": dist_nm, "time_hr": t_hr, "fuel_gal": gal, "path": path})
                heapq.heappush(pq, (new_t, nxt_ap.icao))

    if arr.icao not in best_time:
        return {
            "type": "no_route",
            "message": "No route found under current constraints.",
            "suggestions": [
                "Increase max detour factor",
                "Increase max MSL",
                "Decrease min AGL",
            ],
            "debug": {
                "max_leg_nm": max_leg_nm,
                "expanded_nodes": expanded,
                "stop_candidates": len(stop_candidates),
            },
        }

    # reconstruct legs
    legs_rev: List[dict] = []
    cur = arr.icao
    while cur != dep.icao:
        p = prev.get(cur)
        if not p:
            break
        cur, leg = p
        legs_rev.append(leg)

    legs = list(reversed(legs_rev))
    total_dist = sum(l["dist_nm"] for l in legs)
    total_time = sum(l["time_hr"] for l in legs)
    stops = [l["to"] for l in legs[:-1]]

    route_type = "direct" if len(legs) == 1 else "multi_stop"

    return {
        "type": route_type,
        "legs": legs,
        "total_time_hr": total_time,
        "total_dist_nm": total_dist,
        "stops": stops,
    }


# ── airspace rasterization helpers ────────────────────────────────────

def _point_in_polygon(px: float, py: float, ring: list) -> bool:
    """Ray-casting point-in-polygon test.  ring = [[x,y], ...]."""
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


# Vertical clearance buffer for flying under/over airspace (feet)
_AIRSPACE_VERTICAL_BUFFER_FT = 100


def _rasterize_airspace(grid: GridSpec, avoid_classes: list[str],
                        passable: list[list[bool]],
                        elev_ft: list[float],
                        max_msl_ft: float,
                        min_agl_ft: float) -> Optional[list[list[float]]]:
    """Query airspace DB for polygons in grid bbox and return 2D cost grid.

    Checks each cell's altitude range against the airspace floor/ceiling.
    If the aircraft can fly 100 ft below the airspace floor (descending to
    min AGL) or 100 ft above its ceiling (climbing to max MSL), the cell
    is left clear — avoiding massive lateral detours.

    Prohibited (P) and Restricted (R) areas that cannot be vertically
    avoided set passable=False.  Other avoided classes get a high cost
    multiplier (20).  Returns None if no airspace overlaps the grid.
    """
    if not avoid_classes or not os.path.exists(_AIRSPACE_DB):
        return None

    min_lat = grid.lat0
    max_lat = grid.lat0 + (grid.n_lat - 1) * grid.dlat
    min_lon = grid.lon0
    max_lon = grid.lon0 + (grid.n_lon - 1) * grid.dlon

    placeholders = ",".join("?" for _ in avoid_classes)
    sql = f"""
        SELECT class, geometry, lower_alt, lower_code, upper_alt, upper_code
        FROM airspace
        WHERE class IN ({placeholders})
          AND max_lat >= ? AND min_lat <= ?
          AND max_lon >= ? AND min_lon <= ?
    """
    conn = sqlite3.connect(_AIRSPACE_DB)
    try:
        rows = conn.execute(sql, avoid_classes + [min_lat, max_lat, min_lon, max_lon]).fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    buf = _AIRSPACE_VERTICAL_BUFFER_FT
    cost = [[0.0] * grid.n_lon for _ in range(grid.n_lat)]
    has_any = False

    for cls, geom_str, lower_alt, lower_code, upper_alt, upper_code in rows:
        geom = json.loads(geom_str)
        gtype = geom.get("type", "")
        # normalise to list of rings (outer only — ignoring holes for simplicity)
        if gtype == "Polygon":
            rings = [geom["coordinates"][0]]
        elif gtype == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            continue

        is_hard_block = cls in ("P", "R")
        penalty = 20.0  # high cost multiplier for avoidance

        # Pre-compute airspace altitude bounds (may be None if data missing)
        has_alt_data = (lower_alt is not None and upper_alt is not None)

        for ring in rings:
            # quick bbox reject per ring
            rlons = [c[0] for c in ring]
            rlats = [c[1] for c in ring]
            rmin_lat, rmax_lat = min(rlats), max(rlats)
            rmin_lon, rmax_lon = min(rlons), max(rlons)

            # grid row/col range that might overlap
            i0 = max(0, int((rmin_lat - grid.lat0) / grid.dlat))
            i1 = min(grid.n_lat - 1, int((rmax_lat - grid.lat0) / grid.dlat) + 1)
            j0 = max(0, int((rmin_lon - grid.lon0) / grid.dlon))
            j1 = min(grid.n_lon - 1, int((rmax_lon - grid.lon0) / grid.dlon) + 1)

            for i in range(i0, i1 + 1):
                lat = grid.lat0 + i * grid.dlat
                for j in range(j0, j1 + 1):
                    lon = grid.lon0 + j * grid.dlon
                    if not _point_in_polygon(lon, lat, ring):
                        continue

                    # ── Vertical escape check ──
                    if has_alt_data:
                        k = i * grid.n_lon + j
                        terrain_ft = elev_ft[k]
                        min_flight_alt = terrain_ft + min_agl_ft
                        max_flight_alt = max_msl_ft

                        # Airspace floor MSL
                        lc = (lower_code or "").upper()
                        if lc == "SFC":
                            floor_msl = terrain_ft
                        elif lc == "AGL":
                            floor_msl = terrain_ft + lower_alt
                        else:  # MSL (default)
                            floor_msl = lower_alt

                        # Airspace ceiling MSL
                        uc = (upper_code or "").upper()
                        if uc == "FL":
                            ceiling_msl = upper_alt * 100
                        elif uc == "AGL":
                            ceiling_msl = terrain_ft + upper_alt
                        else:  # MSL (default)
                            ceiling_msl = upper_alt

                        # Can fly 100 ft below the floor?
                        can_go_under = min_flight_alt <= floor_msl - buf
                        # Can fly 100 ft above the ceiling?
                        can_go_over = max_flight_alt >= ceiling_msl + buf

                        if can_go_under or can_go_over:
                            continue  # altitude escape — no conflict

                    has_any = True
                    if is_hard_block:
                        passable[i][j] = False
                    else:
                        cost[i][j] = max(cost[i][j], penalty)

    return cost if has_any else None


def _rasterize_obstacles(grid: GridSpec, passable: list[list[bool]],
                         max_msl_ft: float, radius_nm: float,
                         clearance_ft: float) -> None:
    """Mark grid cells near tall obstacles as impassable.

    An obstacle is relevant when its AMSL + clearance_ft > max_msl_ft — meaning
    the aircraft can't fly over it with enough clearance at its planned altitude.
    A circle of `radius_nm` around each such obstacle is blocked.
    """
    if radius_nm <= 0 or not os.path.exists(_OBSTACLE_DB):
        return

    min_lat = grid.lat0
    max_lat = grid.lat0 + (grid.n_lat - 1) * grid.dlat
    min_lon = grid.lon0
    max_lon = grid.lon0 + (grid.n_lon - 1) * grid.dlon

    # Only care about obstacles whose top + clearance exceeds our flying altitude
    min_amsl = max_msl_ft - clearance_ft

    conn = sqlite3.connect(_OBSTACLE_DB)
    try:
        rows = conn.execute(
            "SELECT lat, lon, amsl FROM obstacles "
            "WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ? AND amsl >= ?",
            (min_lat, max_lat, min_lon, max_lon, min_amsl)
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return

    # Convert radius from NM to grid cell counts (approximate)
    radius_km = radius_nm * 1.852
    radius_cells_lat = radius_km * _deg_per_km_lat() / grid.dlat
    radius_cells_lon = radius_km * _deg_per_km_lon(0.5 * (min_lat + max_lat)) / grid.dlon
    r2_lat = radius_cells_lat ** 2
    r2_lon = radius_cells_lon ** 2

    for olat, olon, oamsl in rows:
        ci = int(round((olat - grid.lat0) / grid.dlat))
        cj = int(round((olon - grid.lon0) / grid.dlon))
        ri = int(math.ceil(radius_cells_lat))
        rj = int(math.ceil(radius_cells_lon))

        i0 = max(0, ci - ri)
        i1 = min(grid.n_lat - 1, ci + ri)
        j0 = max(0, cj - rj)
        j1 = min(grid.n_lon - 1, cj + rj)

        for i in range(i0, i1 + 1):
            di = i - ci
            for j in range(j0, j1 + 1):
                dj = j - cj
                # Elliptical check (accounts for lat/lon scale difference)
                if (di * di) / r2_lat + (dj * dj) / r2_lon <= 1.0:
                    passable[i][j] = False


def terrain_avoid_leg_streaming(
    a: Airport,
    b: Airport,
    max_msl_ft: float,
    min_agl_ft: float,
    max_detour_factor: float,
    cell_km: float = 1.0,
    initial_margin_km: float = 40.0,
    margin_step_km: float = 40.0,
    max_margin_km: float = 400.0,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_speed_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    avoid_airspace: list[str] | None = None,
    obstacle_radius_nm: float = 0,
    obstacle_clearance_ft: float = 500,
    prev_point: tuple[float, float] | None = None,
):
    """Generator that yields A* exploration events for one leg.

    When max_climb_fpm/max_descent_fpm > 0, enforces climb/descent rate
    limits per grid edge (terrain-following constraint).

    Yields dicts:
      {"type": "grid", "bounds": [sw_lat, sw_lon, ne_lat, ne_lon]}
      {"type": "explore", "cells": [[lat, lon], ...]}
      {"type": "path", "coords": [[lat, lon], ...], "dist_nm": float}
      {"type": "no_path"}
    """
    # Build avoidance tag for cache key differentiation
    _avoid_sorted = sorted(avoid_airspace) if avoid_airspace else []
    _atag = ",".join(_avoid_sorted)
    if obstacle_radius_nm > 0:
        _atag += f"|O{obstacle_radius_nm:.1f}_{obstacle_clearance_ft:.0f}"
    if prev_point is not None:
        _atag += f"|PP{prev_point[0]:.2f},{prev_point[1]:.2f}"
    avoidance_tag = _atag

    # ── persistent cache lookup (instant return, no animation) ──
    cached = route_cache.get_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor,
                                 max_climb_fpm, max_descent_fpm,
                                 climb_speed_kt, descent_speed_kt,
                                 avoidance_tag=avoidance_tag)
    if cached is not None:
        if cached.dist_nm == float("inf"):
            yield {"type": "no_path"}
        else:
            yield {"type": "path", "coords": cached.path, "dist_nm": cached.dist_nm, "cached": True}
        return

    from mvp_backend.grid_astar import astar_path_streaming

    provider = SRTMProvider(cache_dir=os.path.join(ROOT, "mvp_backend", "srtm_cache"))

    direct_nm = _direct_nm(a, b)
    direct_km = direct_nm * 1.852
    detour_limit_nm = max_detour_factor * direct_nm
    ceiling_ft = max_msl_ft - min_agl_ft

    mid_lat = 0.5 * (a.lat + b.lat)
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    # Scale initial margin so the grid captures alternate routes around
    # large obstacles (e.g. mountain ranges perpendicular to the track).
    margin_km = max(initial_margin_km, direct_km * 0.55)
    while margin_km <= max_margin_km:
        # Include prev_point in grid extent so approach direction is captured
        lats = [a.lat, b.lat]
        lons = [a.lon, b.lon]
        if prev_point is not None:
            lats.append(prev_point[0])
            lons.append(prev_point[1])
        min_lat = min(lats) - margin_km * _deg_per_km_lat()
        max_lat = max(lats) + margin_km * _deg_per_km_lat()
        min_lon = min(lons) - margin_km * _deg_per_km_lon(mid_lat)
        max_lon = max(lons) + margin_km * _deg_per_km_lon(mid_lat)

        n_lat = int(math.ceil((max_lat - min_lat) / dlat)) + 1
        n_lon = int(math.ceil((max_lon - min_lon) / dlon)) + 1

        # Adaptive cell coarsening: if grid too large, increase cell size
        _MAX_CELLS = 500_000
        _dlat, _dlon = dlat, dlon
        while n_lat * n_lon > _MAX_CELLS:
            _dlat *= 1.5
            _dlon *= 1.5
            n_lat = int(math.ceil((max_lat - min_lat) / _dlat)) + 1
            n_lon = int(math.ceil((max_lon - min_lon) / _dlon)) + 1

        grid = GridSpec(lat0=min_lat, lon0=min_lon, n_lat=n_lat, n_lon=n_lon, dlat=_dlat, dlon=_dlon)

        yield {"type": "grid", "bounds": [min_lat, min_lon, max_lat, max_lon]}

        points = [grid.idx_to_latlon(i, j) for i in range(n_lat) for j in range(n_lon)]
        elev_m = provider.get_many_m(points)
        elev_ft = [meters_to_feet(m) if m == m else float("inf") for m in elev_m]

        passable: List[List[bool]] = [[True] * n_lon for _ in range(n_lat)]
        elev_ft_2d: Optional[List[List[float]]] = None
        if max_climb_fpm > 0 or max_descent_fpm > 0:
            elev_ft_2d = [[0.0] * n_lon for _ in range(n_lat)]
        k = 0
        for i in range(n_lat):
            row = passable[i]
            for j in range(n_lon):
                row[j] = elev_ft[k] <= ceiling_ft
                if elev_ft_2d is not None:
                    elev_ft_2d[i][j] = elev_ft[k]
                k += 1

        start = grid.latlon_to_idx(a.lat, a.lon)
        goal = grid.latlon_to_idx(b.lat, b.lon)
        if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
            margin_km += margin_step_km
            continue

        # rasterize avoided airspace into cost grid (and block Prohibited)
        airspace_cost_2d = None
        if avoid_airspace:
            airspace_cost_2d = _rasterize_airspace(
                grid, avoid_airspace, passable,
                elev_ft=elev_ft, max_msl_ft=max_msl_ft, min_agl_ft=min_agl_ft,
            )
            # re-check start/goal after Prohibited marking
            if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
                margin_km += margin_step_km
                continue

        # rasterize obstacles into passable grid
        if obstacle_radius_nm > 0:
            _rasterize_obstacles(grid, passable, max_msl_ft, obstacle_radius_nm, obstacle_clearance_ft)
            if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
                margin_km += margin_step_km
                continue

        for event in astar_path_streaming(grid, passable, start, goal, yield_every=30,
                                           elev_ft=elev_ft_2d,
                                           max_climb_fpm=max_climb_fpm,
                                           max_descent_fpm=max_descent_fpm,
                                           cruise_kt=cruise_speed_kt,
                                           climb_speed_kt=climb_speed_kt,
                                           descent_speed_kt=descent_speed_kt,
                                           airspace_cost=airspace_cost_2d):
            if event["type"] == "path" and event["dist_nm"] > detour_limit_nm:
                break  # too long, try wider margin
            if event["type"] == "no_path":
                break  # no path in this margin, try wider
            yield event
            if event["type"] == "path":
                route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor, event["dist_nm"], event["coords"],
                                    max_climb_fpm, max_descent_fpm,
                                    climb_speed_kt, descent_speed_kt,
                                    avoidance_tag=avoidance_tag)
                return

        margin_km += margin_step_km

    # ── Failure diagnostics ──
    # Determine *why* no path was found so the UI can display a useful hint.
    diag_reason = "unknown"
    diag_detail = ""
    # Check if terrain ceiling is the blocker: sample the direct path midpoint
    mid_lat = 0.5 * (a.lat + b.lat)
    mid_lon = 0.5 * (a.lon + b.lon)
    try:
        mid_elev = provider.get_many_m([(mid_lat, mid_lon)])[0]
        mid_elev_ft = meters_to_feet(mid_elev) if mid_elev == mid_elev else 0
        if mid_elev_ft > ceiling_ft:
            diag_reason = "terrain_ceiling"
            diag_detail = (
                f"Terrain at midpoint ({mid_lat:.2f}, {mid_lon:.2f}) rises to "
                f"{int(mid_elev_ft):,}' MSL, above your {int(ceiling_ft):,}' "
                f"effective ceiling ({int(max_msl_ft):,}' MSL − {int(min_agl_ft):,}' AGL)."
            )
    except Exception:
        pass
    if diag_reason == "unknown":
        # Check restricted/prohibited airspace blocking
        if avoid_airspace:
            diag_reason = "restricted_airspace"
            diag_detail = (
                f"Restricted/prohibited airspace between {a.icao} and {b.icao} "
                f"may block all routes within {max_detour_factor:.1f}x detour limit."
            )
        else:
            diag_reason = "detour_limit"
            diag_detail = (
                f"All terrain-avoiding paths exceed the {max_detour_factor:.1f}x "
                f"detour limit ({int(detour_limit_nm)} NM for {int(direct_nm)} NM direct)."
            )

    route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft, max_detour_factor, None, None,
                        max_climb_fpm, max_descent_fpm,
                        climb_speed_kt, descent_speed_kt,
                        avoidance_tag=avoidance_tag)
    yield {"type": "no_path", "reason": diag_reason, "detail": diag_detail}
