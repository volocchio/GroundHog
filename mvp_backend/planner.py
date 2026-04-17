from __future__ import annotations

import csv
import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from mvp_backend.grid_astar import GridSpec, astar_path, astar_path_streaming, path_nm, smooth_path
from mvp_backend.terrain_provider import meters_to_feet
from mvp_backend.srtm_local import SRTMProvider
from mvp_backend import route_cache
from mvp_backend import terrain_intel
from mvp_backend.terrain_intel import DEFAULT_AGL_FT as _INTEL_AGL


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
    # Add FAA LID aliases: K-prefixed US ICAO -> 3-char LID (e.g. KSZT -> SZT)
    for icao_code, ap_obj in list(out.items()):
        if len(icao_code) == 4 and icao_code.startswith("K") and icao_code[1:].isalnum():
            lid_alias = icao_code[1:]
            if lid_alias not in out:
                out[lid_alias] = ap_obj
    # Reverse aliases: 3-char US LID -> K-prefixed ICAO (e.g. E60 -> KE60)
    for code, ap_obj in list(out.items()):
        if len(code) == 3 and code.isalnum():
            k_alias = "K" + code
            if k_alias not in out:
                out[k_alias] = ap_obj
    return out


def _deg_per_km_lat() -> float:
    return 1.0 / 110.574


def _deg_per_km_lon(lat: float) -> float:
    return 1.0 / (111.320 * math.cos(math.radians(lat)) + 1e-9)


def _direct_nm(a: Airport, b: Airport) -> float:
    # reuse haversine from grid_astar
    from mvp_backend.grid_astar import _haversine_nm
    return _haversine_nm(a.lat, a.lon, b.lat, b.lon)


def _densify_latlon(path: List[Tuple[float, float]], max_step_nm: float = 2.0) -> List[Tuple[float, float]]:
    """Interpolate lat/lon points along a path so segments are at most max_step_nm apart."""
    from mvp_backend.grid_astar import _haversine_nm
    if len(path) <= 1:
        return list(path)
    result = [path[0]]
    for k in range(len(path) - 1):
        lat0, lon0 = path[k]
        lat1, lon1 = path[k + 1]
        seg_nm = _haversine_nm(lat0, lon0, lat1, lon1)
        n_sub = max(1, int(seg_nm / max_step_nm))
        for s in range(1, n_sub):
            t = s / n_sub
            result.append((lat0 + (lat1 - lat0) * t, lon0 + (lon1 - lon0) * t))
        result.append(path[k + 1])
    return result


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
    timeout_s: float = 90.0,
) -> Optional[LegResult]:
    """Find terrain-avoiding path between airports using grid A*.

    Expands a bounding-box margin until a path is found with acceptable detour.
    When max_climb_fpm/max_descent_fpm > 0, also enforces climb/descent rate
    limits per grid edge (terrain-following constraint).
    """
    _deadline = time.monotonic() + timeout_s
    # ── terrain intelligence quick-reject ──
    # Only hard-reject when the precomputed data found a *specific* minimum
    # viable altitude (proving the data covers the corridor).  When
    # min_viable_msl is None ("no path at any altitude") the precomputed
    # 2 km BFS likely has gaps — let fine-grained A* try.
    if max_msl_ft > 0:
        effective_ceiling = max_msl_ft - min_agl_ft
        intel_msl = effective_ceiling + _INTEL_AGL
        vi = terrain_intel.check_viability(
            a.lat, a.lon, b.lat, b.lon, intel_msl, _INTEL_AGL)
        if (vi["analyzed"] and vi["viable"] is False
                and vi.get("min_viable_msl") is not None):
            route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft,
                                max_detour_factor, None, None,
                                max_climb_fpm, max_descent_fpm,
                                climb_speed_kt, descent_speed_kt)
            return None

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

    # Adaptive cell size: coarsen for long legs to keep grid manageable
    eff_cell_km = cell_km
    if direct_km > 400:
        eff_cell_km = max(cell_km, 2.0)
    elif direct_km > 200:
        eff_cell_km = max(cell_km, 1.5)

    dlat = eff_cell_km * _deg_per_km_lat()
    dlon = eff_cell_km * _deg_per_km_lon(mid_lat)

    # Scale initial margin so the grid captures alternate routes around
    # large obstacles (e.g. mountain ranges perpendicular to the track).
    margin_km = max(initial_margin_km, direct_km * 0.55)
    while margin_km <= max_margin_km:
        if time.monotonic() > _deadline:
            break  # timeout — treat as failure
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
            _dlat *= 1.25
            _dlon *= 1.25
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

        if time.monotonic() > _deadline:
            break  # timeout after grid build

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

        # Smooth out grid-aligned zig-zags
        path_idx = smooth_path(grid, passable, path_idx,
                               elev_ft=elev_ft_2d,
                               max_climb_fpm=max_climb_fpm,
                               max_descent_fpm=max_descent_fpm,
                               cruise_kt=cruise_speed_kt,
                               climb_speed_kt=climb_speed_kt,
                               descent_speed_kt=descent_speed_kt)

        dist_nm = path_nm(grid, path_idx)
        if dist_nm <= detour_limit_nm:
            # Convert smoothed waypoints to lat/lon, then interpolate
            # at ~2 NM intervals using true lat/lon (not grid-snapped)
            smoothed_latlon = [grid.idx_to_latlon(i, j) for i, j in path_idx]
            path_latlon = _densify_latlon(smoothed_latlon, max_step_nm=2.0)
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


def leg_fuel_ok(dist_nm: float, cruise_speed_kt: float, usable_fuel_gal: float, burn_gph: float, reserve_min: float) -> tuple[bool, float, float]:
    time_hr = dist_nm / max(1e-6, cruise_speed_kt)
    reserve_gal = (reserve_min / 60.0) * burn_gph
    needed_gal = time_hr * burn_gph + reserve_gal
    return (needed_gal <= usable_fuel_gal, time_hr, needed_gal)


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

    # Realistic planning cushion for terrain detours.  At low ceilings
    # (< 8000 ft) mountain terrain forces much larger detours, so scale up
    # the cushion to keep the fuel-stop planner from promising legs that
    # the terrain A* can't deliver.
    if max_msl_ft > 0 and max_msl_ft < 8000:
        # Linear ramp: 1.15 @ 8000 → 1.45 @ 4000 → 1.60 @ 3000
        terrain_extra = 0.075 * (8000 - max_msl_ft) / 1000.0
        planning_detour = min(max_detour_factor, 1.15 + terrain_extra)
    else:
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
    # Deduplicate: FAA LID aliases cause same Airport under multiple keys
    stop_candidates: List[Airport] = []
    _seen_ids: set = set()
    for ap in airports.values():
        if id(ap) in _seen_ids:
            continue
        _seen_ids.add(id(ap))
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

    # Cross-track helper: approximate perpendicular distance from the
    # dep→arr great-circle line, in NM.  Used to penalise stops that are
    # far to the side of the intended route even if they aren't "behind".
    _xt_mid_lat = 0.5 * (dep.lat + arr.lat)
    _xt_cos = math.cos(math.radians(_xt_mid_lat))
    _xt_dx_arr = (arr.lon - dep.lon) * _xt_cos
    _xt_dy_arr = arr.lat - dep.lat
    _xt_track_len = math.sqrt(_xt_dx_arr ** 2 + _xt_dy_arr ** 2) or 1e-12

    def _cross_track_nm(ap: Airport) -> float:
        dx = (ap.lon - dep.lon) * _xt_cos
        dy = ap.lat - dep.lat
        cross_deg = abs(dx * _xt_dy_arr - dy * _xt_dx_arr) / _xt_track_len
        return cross_deg * 60.0  # rough deg → NM

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

            neigh: List[tuple[float, float, Airport]] = []  # (d, t_hr, ap)
            if d_cur_to_arr <= cur_radius:
                ok_a, t_a, _ = leg_fuel_ok(d_cur_to_arr * planning_detour,
                                           cruise_speed_kt, cur_fuel, burn_gph, reserve_min)
                if ok_a and not _is_blocked(cur_code, arr.icao):
                    neigh.append((d_cur_to_arr, t_a, arr))

            for ap in stop_candidates:
                if ap.icao == cur_code:
                    continue
                d = _direct_nm(cur_ap, ap)
                if d <= cur_radius:
                    # Reject stops that move significantly farther from the
                    # destination than we are now.  Allow up to 15% backtrack
                    # (covers slight detours for terrain) but reject anything
                    # that's a complete double-back.
                    d_ap_to_arr = _direct_nm(ap, arr)
                    if d_ap_to_arr > d_cur_to_arr * 1.15 + 20:
                        continue
                    if _is_blocked(cur_code, ap.icao):
                        continue
                    ok, t_hr, _ = leg_fuel_ok(d * planning_detour,
                                              cruise_speed_kt, cur_fuel, burn_gph, reserve_min)
                    if not ok:
                        continue
                    neigh.append((d, t_hr, ap))

            # Sort by distance-to-destination (forward progress) instead of
            # distance-from-current, so forward airports are explored first.
            neigh.sort(key=lambda x: _direct_nm(x[2], arr))
            neigh = neigh[:max_neighbors]

            for d, t_hr, nxt_ap in neigh:
                penalty = 0.0 if nxt_ap.icao == arr.icao else stop_penalty_hr
                extra = 0.0
                if edge_penalty:
                    extra = edge_penalty.get((cur_code, nxt_ap.icao), 0.0)

                # Backtrack penalty: if this hop moves AWAY from the
                # destination, add a time cost proportional to how much
                # backward progress it represents.  A complete double-back
                # should be nearly prohibitive.
                d_nxt_to_arr = _direct_nm(nxt_ap, arr)
                backtrack_nm = d_nxt_to_arr - d_cur_to_arr
                backtrack_penalty = 0.0
                if backtrack_nm > 0 and cruise_speed_kt > 0:
                    # Heavy penalty: 8× the backward distance as hours.
                    # This strongly discourages any stop that moves away
                    # from the destination.
                    backtrack_penalty = (backtrack_nm * 8.0) / cruise_speed_kt

                # Off-track penalty: stops far from the dep→arr line are
                # likely to produce zig-zag multi-leg routes.
                offtrack_penalty = 0.0
                if nxt_ap.icao != arr.icao and cruise_speed_kt > 0:
                    xt = _cross_track_nm(nxt_ap)
                    offtrack_penalty = (xt * 3.0) / cruise_speed_kt

                new_g = cur_g + t_hr + penalty + extra + backtrack_penalty + offtrack_penalty
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

    # Build candidate stop list once (deduplicate FAA LID aliases)
    stop_candidates: List[Airport] = []
    _seen_ids2: set = set()
    for ap in airports.values():
        if id(ap) in _seen_ids2:
            continue
        _seen_ids2.add(id(ap))
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
                d_ap_to_arr = _direct_nm(ap, arr)
                if d_ap_to_arr > d_cur_to_arr * 1.15 + 20:
                    continue
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
                backtrack_penalty = (backtrack_nm * 8.0) / cruise_speed_kt

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

def _airspace_floor_msl(lower_alt, lower_code, terrain_ft: float) -> float:
    """Convert airspace floor to MSL feet."""
    lc = (lower_code or "").upper()
    if lc == "SFC":
        return terrain_ft
    if lc == "AGL":
        return terrain_ft + lower_alt
    if lc in ("STD", "FL"):
        return lower_alt * 100  # Flight level
    return lower_alt  # MSL default


def _airspace_ceiling_msl(upper_alt, upper_code, terrain_ft: float) -> float:
    """Convert airspace ceiling to MSL feet."""
    uc = (upper_code or "").upper()
    if uc in ("FL", "STD"):
        return upper_alt * 100
    if uc == "AGL":
        return terrain_ft + upper_alt
    if uc == "UNLTD" or upper_alt is None or upper_alt < 0:
        return 99999  # Unlimited
    return upper_alt  # MSL default


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
        # Class-dependent penalty: hard-blocked classes bypass this,
        # military classes get moderate penalty, controlled airspace
        # gets a lighter penalty (pilots can request transition).
        _AIRSPACE_PENALTIES = {
            "MOA": 8.0, "W": 8.0, "A": 6.0,
            "B": 4.0, "C": 3.0, "D": 2.0,
        }
        penalty = _AIRSPACE_PENALTIES.get(cls, 10.0)

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

                        floor_msl = _airspace_floor_msl(lower_alt, lower_code, terrain_ft)
                        ceiling_msl = _airspace_ceiling_msl(upper_alt, upper_code, terrain_ft)

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


def _detect_water_grid(n_lat: int, n_lon: int, elev_ft: list) -> list[list[bool]]:
    """Detect water cells in a flat elevation array.

    Water is detected two ways:
    1. Ocean/void: elevation <= 0 or inf (SRTM void).
    2. Inland water (lakes/reservoirs): SRTM fills water bodies with a
       single constant value.  A cell whose elevation exactly matches ALL
       cardinal neighbors is almost certainly water (land always has ≥1 m
       SRTM noise).  We require a 3-cell connected flood from seeds to
       avoid flagging isolated flat pixels.
    """
    # Build 2D elevation view
    e2d = [[0.0] * n_lon for _ in range(n_lat)]
    k = 0
    for i in range(n_lat):
        for j in range(n_lon):
            e2d[i][j] = elev_ft[k]
            k += 1

    is_water = [[False] * n_lon for _ in range(n_lat)]

    # Pass 1: ocean/void
    for i in range(n_lat):
        for j in range(n_lon):
            e = e2d[i][j]
            if e <= 0 or e == float("inf"):
                is_water[i][j] = True

    # Pass 2: inland water — cells matching ALL cardinal neighbors exactly
    flat = [[False] * n_lon for _ in range(n_lat)]
    for i in range(n_lat):
        for j in range(n_lon):
            if is_water[i][j]:
                continue
            e = e2d[i][j]
            match_count = 0
            neighbour_count = 0
            for di, dj in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                ni, nj = i + di, j + dj
                if 0 <= ni < n_lat and 0 <= nj < n_lon:
                    neighbour_count += 1
                    if e2d[ni][nj] == e:
                        match_count += 1
            # Must match all available cardinal neighbors (at least 2)
            if neighbour_count >= 2 and match_count == neighbour_count:
                flat[i][j] = True

    # Flood-fill from flat seeds: only keep connected clusters ≥ 3 cells
    from collections import deque
    visited = [[False] * n_lon for _ in range(n_lat)]
    for i in range(n_lat):
        for j in range(n_lon):
            if not flat[i][j] or visited[i][j]:
                continue
            # BFS to find connected component of same-elevation flat cells
            cluster = []
            q = deque()
            q.append((i, j))
            visited[i][j] = True
            ref_e = e2d[i][j]
            while q:
                ci, cj = q.popleft()
                cluster.append((ci, cj))
                for di, dj in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    ni, nj = ci + di, cj + dj
                    if 0 <= ni < n_lat and 0 <= nj < n_lon and not visited[ni][nj]:
                        if flat[ni][nj] and e2d[ni][nj] == ref_e:
                            visited[ni][nj] = True
                            q.append((ni, nj))
            if len(cluster) >= 3:
                for ci, cj in cluster:
                    is_water[ci][cj] = True

    return is_water


def _build_water_cost(grid: GridSpec, elev_ft: list, passable: list[list[bool]],
                      glide_ratio: float, max_msl_ft: float,
                      water_risk: float) -> tuple:
    """Build a 2D cost grid penalizing water cells beyond autorotation glide range.

    Water is detected via _detect_water_grid (ocean voids AND inland lakes).
    Uses multi-source BFS from all land cells to compute distance-to-shore
    for each water cell, then applies cost based on whether the cell is
    within glide range.

    water_risk: 0=strict (glide to shore), 25/50/75=relaxed, 100=ignore.
    Returns (full_cost, smoother_cost) or (None, None).
      full_cost      — used by A* (mild penalty on within-glide, heavy beyond)
      smoother_cost  — used by path smoother (zero within-glide, heavy beyond)
    """
    if water_risk >= 100:
        return None, None

    n_lat = grid.n_lat
    n_lon = grid.n_lon

    # Risk multiplier on effective glide range
    if water_risk <= 0:
        glide_mult = 1.0
    elif water_risk <= 25:
        glide_mult = 1.5
    elif water_risk <= 50:
        glide_mult = 2.0
    else:
        glide_mult = 3.0

    # Glide range in NM from cruise altitude (over water, terrain ~ 0)
    glide_range_nm = max_msl_ft * glide_ratio / 6076.12 * glide_mult

    # Cell size in NM (approximate)
    mid_lat = grid.lat0 + (n_lat / 2) * grid.dlat
    cell_nm_lat = grid.dlat / _deg_per_km_lat() / 1.852
    cell_nm_lon = grid.dlon / _deg_per_km_lon(mid_lat) / 1.852
    cell_nm = 0.5 * (cell_nm_lat + cell_nm_lon)

    glide_range_cells = glide_range_nm / cell_nm if cell_nm > 0 else 0

    is_water = _detect_water_grid(n_lat, n_lon, elev_ft)
    has_water = any(is_water[i][j] for i in range(n_lat) for j in range(n_lon))

    if not has_water:
        return None, None

    # BFS from all land cells to compute distance-to-shore for water cells
    from collections import deque
    INF = float("inf")
    dist = [[INF] * n_lon for _ in range(n_lat)]
    q = deque()

    # Seed: all land cells at distance 0
    for i in range(n_lat):
        for j in range(n_lon):
            if not is_water[i][j]:
                dist[i][j] = 0.0
                q.append((i, j))

    # 8-connected BFS
    while q:
        ci, cj = q.popleft()
        cd = dist[ci][cj]
        for di in (-1, 0, 1):
            ni = ci + di
            if ni < 0 or ni >= n_lat:
                continue
            for dj in (-1, 0, 1):
                if di == 0 and dj == 0:
                    continue
                nj = cj + dj
                if nj < 0 or nj >= n_lon:
                    continue
                step = 1.414 if (di != 0 and dj != 0) else 1.0
                nd = cd + step
                if nd < dist[ni][nj]:
                    dist[ni][nj] = nd
                    q.append((ni, nj))

    # Build cost grids
    WATER_BASE = 2.0       # mild cost for any water cell (A* only)
    WATER_BEYOND = 200.0   # very heavy cost for cells beyond glide range
    cost = [[0.0] * n_lon for _ in range(n_lat)]
    smooth_cost = [[0.0] * n_lon for _ in range(n_lat)]
    has_cost = False
    for i in range(n_lat):
        for j in range(n_lon):
            if not is_water[i][j]:
                continue
            d = dist[i][j]
            if d <= glide_range_cells:
                cost[i][j] = WATER_BASE
                # smoother: zero cost within glide range — safe to shortcut
                smooth_cost[i][j] = 0.0
            else:
                # Scale penalty by how far beyond glide range
                excess = (d - glide_range_cells) / max(1.0, glide_range_cells)
                beyond = WATER_BEYOND * min(excess, 3.0)
                cost[i][j] = WATER_BASE + beyond
                # smoother gets the same heavy penalty — blocks dangerous shortcuts
                smooth_cost[i][j] = WATER_BASE + beyond
            has_cost = True

    return (cost, smooth_cost) if has_cost else (None, None)


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
    avoid_borders: bool = True,
    glide_ratio: float = 0,
    water_risk: float = 100,
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
    if avoid_borders:
        _atag += "|BORDERS"
    if water_risk < 100 and glide_ratio > 0:
        _atag += f"|W{water_risk:.0f}G{glide_ratio:.1f}"
    avoidance_tag = _atag

    # ── terrain intelligence quick-reject ──
    # Only hard-reject when the precomputed data found a *specific* minimum
    # viable altitude (proving the data covers the corridor).  When
    # min_viable_msl is None ("no path at any altitude") the precomputed
    # 2 km BFS likely has gaps — let fine-grained A* try.
    if max_msl_ft > 0:
        effective_ceiling = max_msl_ft - min_agl_ft
        intel_msl = effective_ceiling + _INTEL_AGL
        vi = terrain_intel.check_viability(
            a.lat, a.lon, b.lat, b.lon, intel_msl, _INTEL_AGL)
        if (vi["analyzed"] and vi["viable"] is False
                and vi.get("min_viable_msl") is not None):
            user_min_msl = vi["min_viable_msl"] - _INTEL_AGL + min_agl_ft
            route_cache.put_leg(a.icao, b.icao, max_msl_ft, min_agl_ft,
                                max_detour_factor, None, None,
                                max_climb_fpm, max_descent_fpm,
                                climb_speed_kt, descent_speed_kt,
                                avoidance_tag=avoidance_tag)
            detail = (f"Not viable at {max_msl_ft:.0f}' MSL with {min_agl_ft:.0f}' AGL clearance. "
                      f"Minimum viable: {user_min_msl:.0f}' MSL.")
            yield {"type": "no_path", "reason": "terrain_intel",
                   "detail": detail,
                   "min_viable_msl": vi.get("min_viable_msl")}
            return

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
    # Water avoidance may force long detours around large water bodies;
    # relax the detour limit so the planner can find a viable path.
    if water_risk < 100 and glide_ratio > 0:
        water_detour = max(max_detour_factor * 2.0, 4.0)
        detour_limit_nm = max(detour_limit_nm, water_detour * direct_nm)
    ceiling_ft = max_msl_ft - min_agl_ft

    mid_lat = 0.5 * (a.lat + b.lat)

    # Adaptive cell size: coarsen for long legs to keep grid manageable
    eff_cell_km = cell_km
    if direct_km > 400:
        eff_cell_km = max(cell_km, 2.0)
    elif direct_km > 200:
        eff_cell_km = max(cell_km, 1.5)

    dlat = eff_cell_km * _deg_per_km_lat()
    dlon = eff_cell_km * _deg_per_km_lon(mid_lat)

    # Scale initial margin so the grid captures alternate routes around
    # large obstacles (e.g. mountain ranges perpendicular to the track).
    margin_km = max(initial_margin_km, direct_km * 0.55)
    # When avoiding water, widen the initial search margin so the grid
    # can encompass routes around large water bodies.
    if water_risk < 100 and glide_ratio > 0:
        margin_km = max(margin_km, direct_km * 1.0)
    while margin_km <= max_margin_km:
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
            _dlat *= 1.25
            _dlon *= 1.25
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

        # ── Border avoidance: mark cells outside CONUS as impassable ──
        if avoid_borders:
            for i in range(n_lat):
                for j in range(n_lon):
                    clat, clon = grid.idx_to_latlon(i, j)
                    if clat > 49.0 or clat < 25.0:
                        passable[i][j] = False

        start = grid.latlon_to_idx(a.lat, a.lon)
        goal = grid.latlon_to_idx(b.lat, b.lon)
        if not passable[start[0]][start[1]] or not passable[goal[0]][goal[1]]:
            margin_km += margin_step_km
            continue

        # rasterize avoided airspace into cost grid (and block Prohibited)
        airspace_cost_2d = None
        airspace_only_cost_2d = None   # for smoother (excludes water cost)
        if avoid_airspace:
            airspace_cost_2d = _rasterize_airspace(
                grid, avoid_airspace, passable,
                elev_ft=elev_ft, max_msl_ft=max_msl_ft, min_agl_ft=min_agl_ft,
            )
            # Keep a copy of airspace-only costs for the smoother so it
            # respects restricted airspace but can straighten over water.
            if airspace_cost_2d is not None:
                airspace_only_cost_2d = [row[:] for row in airspace_cost_2d]
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

        # ── Water avoidance cost ──
        if glide_ratio > 0 and water_risk < 100:
            water_cost_2d, smooth_water_cost_2d = _build_water_cost(
                grid, elev_ft, passable,
                glide_ratio=glide_ratio, max_msl_ft=max_msl_ft,
                water_risk=water_risk,
            )
            if water_cost_2d is not None:
                if airspace_cost_2d is None:
                    airspace_cost_2d = water_cost_2d
                else:
                    for i in range(n_lat):
                        for j in range(n_lon):
                            airspace_cost_2d[i][j] += water_cost_2d[i][j]
            # Merge beyond-glide water cost into the smoother grid so
            # it can't shortcut across dangerous water crossings.
            if smooth_water_cost_2d is not None:
                if airspace_only_cost_2d is None:
                    airspace_only_cost_2d = smooth_water_cost_2d
                else:
                    for i in range(n_lat):
                        for j in range(n_lon):
                            airspace_only_cost_2d[i][j] += smooth_water_cost_2d[i][j]

        # ── Gentle backtrack avoidance ──
        # When prev_point is given (multi-leg route), add a light cost to
        # cells very close to prev_point to discourage (but not block)
        # routing back through the previous departure airport.
        if prev_point is not None:
            prev_lat, prev_lon = prev_point
            cos_mid = math.cos(math.radians(0.5 * (a.lat + prev_lat)))
            # Only penalize cells within ~10 NM of prev_point
            penalty_radius_deg = 10.0 / 60.0  # ~10 NM in degrees lat
            backtrack_cost = [[0.0] * n_lon for _ in range(n_lat)]
            has_any = False
            for i in range(n_lat):
                for j in range(n_lon):
                    clat, clon = grid.idx_to_latlon(i, j)
                    dc_lat = clat - prev_lat
                    dc_lon = (clon - prev_lon) * cos_mid
                    d_deg = math.sqrt(dc_lat * dc_lat + dc_lon * dc_lon)
                    if d_deg < penalty_radius_deg:
                        ratio = 1.0 - (d_deg / penalty_radius_deg)
                        backtrack_cost[i][j] = ratio * 0.3  # gentle nudge
                        has_any = True
            if has_any:
                if airspace_cost_2d is None:
                    airspace_cost_2d = backtrack_cost
                else:
                    for i in range(n_lat):
                        for j in range(n_lon):
                            airspace_cost_2d[i][j] += backtrack_cost[i][j]

        for event in astar_path_streaming(grid, passable, start, goal, yield_every=30,
                                           elev_ft=elev_ft_2d,
                                           max_climb_fpm=max_climb_fpm,
                                           max_descent_fpm=max_descent_fpm,
                                           cruise_kt=cruise_speed_kt,
                                           climb_speed_kt=climb_speed_kt,
                                           descent_speed_kt=descent_speed_kt,
                                           airspace_cost=airspace_cost_2d,
                                           smooth_airspace_cost=airspace_only_cost_2d):
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
    min_viable_msl = None
    # Check terrain intel for min viable MSL (use precomputed AGL)
    if max_msl_ft > 0:
        effective_ceiling = max_msl_ft - min_agl_ft
        intel_msl = effective_ceiling + _INTEL_AGL
        vi = terrain_intel.check_viability(
            a.lat, a.lon, b.lat, b.lon, intel_msl, _INTEL_AGL)
        if vi.get("min_viable_msl"):
            # Convert back to user's AGL reference
            min_viable_msl = vi["min_viable_msl"] - _INTEL_AGL + min_agl_ft
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
    fail_event = {"type": "no_path", "reason": diag_reason, "detail": diag_detail}
    if min_viable_msl is not None:
        fail_event["min_viable_msl"] = min_viable_msl
        fail_event["detail"] += f" Minimum viable MSL: {min_viable_msl:.0f}'."
    yield fail_event
