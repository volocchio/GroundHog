from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Dict, Generator, List, Optional, Tuple


@dataclass(frozen=True)
class GridSpec:
    lat0: float
    lon0: float
    n_lat: int
    n_lon: int
    dlat: float
    dlon: float

    def idx_to_latlon(self, i: int, j: int) -> tuple[float, float]:
        return (self.lat0 + i * self.dlat, self.lon0 + j * self.dlon)

    def latlon_to_idx(self, lat: float, lon: float) -> tuple[int, int]:
        i = int(round((lat - self.lat0) / self.dlat))
        j = int(round((lon - self.lon0) / self.dlon))
        i = max(0, min(self.n_lat - 1, i))
        j = max(0, min(self.n_lon - 1, j))
        return i, j


def _haversine_nm(lat1, lon1, lat2, lon2) -> float:
    R_nm = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    return 2 * R_nm * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def astar_path(
    grid: GridSpec,
    passable: List[List[bool]],
    start: tuple[int, int],
    goal: tuple[int, int],
    elev_ft: Optional[List[List[float]]] = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    airspace_cost: Optional[List[List[float]]] = None,
) -> Optional[List[tuple[int, int]]]:
    """8-connected A* over passable grid.

    When elev_ft + cruise_kt are provided, also enforces climb/descent rate
    limits per edge (terrain-following model).  climb_speed_kt / descent_speed_kt
    override cruise_kt for ascending / descending edges respectively.

    airspace_cost: optional 2D grid of additive cost multipliers per cell.
    """

    def h(a: tuple[int, int], b: tuple[int, int]) -> float:
        lat1, lon1 = grid.idx_to_latlon(*a)
        lat2, lon2 = grid.idx_to_latlon(*b)
        return _haversine_nm(lat1, lon1, lat2, lon2)

    open_heap: list[tuple[float, float, tuple[int, int]]] = []
    heapq.heappush(open_heap, (h(start, goal), 0.0, start))

    came_from: Dict[tuple[int, int], tuple[int, int]] = {}
    gscore: Dict[tuple[int, int], float] = {start: 0.0}

    neighbors = [
        (-1, -1), (-1, 0), (-1, 1),
        (0, -1),           (0, 1),
        (1, -1),  (1, 0),  (1, 1),
    ]

    while open_heap:
        f, g, cur = heapq.heappop(open_heap)
        if cur == goal:
            # reconstruct
            path = [cur]
            while cur in came_from:
                cur = came_from[cur]
                path.append(cur)
            path.reverse()
            return path

        ci, cj = cur
        for di, dj in neighbors:
            ni, nj = ci + di, cj + dj
            if ni < 0 or nj < 0 or ni >= grid.n_lat or nj >= grid.n_lon:
                continue
            if not passable[ni][nj]:
                continue
            # movement cost
            lat1, lon1 = grid.idx_to_latlon(ci, cj)
            lat2, lon2 = grid.idx_to_latlon(ni, nj)
            step = _haversine_nm(lat1, lon1, lat2, lon2)
            # climb/descent rate check (terrain-following model)
            if elev_ft is not None and cruise_kt > 0 and step > 0:
                d_elev = elev_ft[ni][nj] - elev_ft[ci][cj]
                if d_elev > 0 and max_climb_fpm > 0:
                    spd = climb_speed_kt if climb_speed_kt > 0 else cruise_kt
                    time_min = (step / spd) * 60.0
                    if d_elev / time_min > max_climb_fpm:
                        continue
                if d_elev < 0 and max_descent_fpm > 0:
                    spd = descent_speed_kt if descent_speed_kt > 0 else cruise_kt
                    time_min = (step / spd) * 60.0
                    if (-d_elev) / time_min > max_descent_fpm:
                        continue
            # airspace avoidance penalty
            if airspace_cost is not None:
                step *= (1.0 + airspace_cost[ni][nj])
            ng = gscore[cur] + step
            nxt = (ni, nj)
            if ng < gscore.get(nxt, float("inf")):
                came_from[nxt] = cur
                gscore[nxt] = ng
                heapq.heappush(open_heap, (ng + h(nxt, goal), ng, nxt))

    return None


def path_nm(grid: GridSpec, path: List[tuple[int, int]]) -> float:
    if len(path) < 2:
        return 0.0
    total = 0.0
    for (i1, j1), (i2, j2) in zip(path, path[1:]):
        lat1, lon1 = grid.idx_to_latlon(i1, j1)
        lat2, lon2 = grid.idx_to_latlon(i2, j2)
        total += _haversine_nm(lat1, lon1, lat2, lon2)
    return total


def _bresenham_cells(i0: int, j0: int, i1: int, j1: int) -> List[tuple[int, int]]:
    """Return all grid cells along the line from (i0,j0) to (i1,j1)."""
    cells = []
    di = abs(i1 - i0)
    dj = abs(j1 - j0)
    si = 1 if i0 < i1 else -1
    sj = 1 if j0 < j1 else -1
    err = di - dj
    ci, cj = i0, j0
    while True:
        cells.append((ci, cj))
        if ci == i1 and cj == j1:
            break
        e2 = 2 * err
        if e2 > -dj:
            err -= dj
            ci += si
        if e2 < di:
            err += di
            cj += sj
    return cells


def _line_of_sight(
    grid: GridSpec,
    passable: List[List[bool]],
    a: tuple[int, int],
    b: tuple[int, int],
    elev_ft: Optional[List[List[float]]] = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    airspace_cost: Optional[List[List[float]]] = None,
    smooth_airspace_cost: Optional[List[List[float]]] = None,
    max_airspace_cost: float = 0.5,
    max_ridge_excess_ft: float = 1500.0,
) -> bool:
    """Check if all cells on the line from a to b are passable and satisfy
    climb/descent constraints between consecutive cells.

    Also rejects shortcuts where intermediate terrain rises more than
    max_ridge_excess_ft above both endpoints — prevents smoothing from
    cutting straight over mountain ridges that A* originally routed around.
    """
    cells = _bresenham_cells(a[0], a[1], b[0], b[1])
    # Use smooth_airspace_cost (airspace-only, no water) if provided,
    # otherwise fall back to airspace_cost.  This lets the smoother
    # straighten paths over water while still respecting restricted
    # airspace boundaries.
    los_cost = smooth_airspace_cost if smooth_airspace_cost is not None else airspace_cost
    for ci, cj in cells:
        if not passable[ci][cj]:
            return False
        if los_cost is not None and los_cost[ci][cj] > max_airspace_cost:
            return False

    # Reject shortcuts that cross ridges much higher than endpoints
    if elev_ft is not None and len(cells) > 2:
        elev_a = elev_ft[a[0]][a[1]]
        elev_b = elev_ft[b[0]][b[1]]
        base = max(elev_a, elev_b)
        for ci, cj in cells[1:-1]:
            if elev_ft[ci][cj] - base > max_ridge_excess_ft:
                return False

    # Check climb/descent constraints along the shortcut
    if elev_ft is not None and cruise_kt > 0 and len(cells) > 1:
        for k in range(len(cells) - 1):
            ci, cj = cells[k]
            ni, nj = cells[k + 1]
            lat1, lon1 = grid.idx_to_latlon(ci, cj)
            lat2, lon2 = grid.idx_to_latlon(ni, nj)
            step = _haversine_nm(lat1, lon1, lat2, lon2)
            if step <= 0:
                continue
            d_elev = elev_ft[ni][nj] - elev_ft[ci][cj]
            if d_elev > 0 and max_climb_fpm > 0:
                spd = climb_speed_kt if climb_speed_kt > 0 else cruise_kt
                time_min = (step / spd) * 60.0
                if d_elev / time_min > max_climb_fpm:
                    return False
            if d_elev < 0 and max_descent_fpm > 0:
                spd = descent_speed_kt if descent_speed_kt > 0 else cruise_kt
                time_min = (step / spd) * 60.0
                if (-d_elev) / time_min > max_descent_fpm:
                    return False
    return True


def smooth_path(
    grid: GridSpec,
    passable: List[List[bool]],
    path: List[tuple[int, int]],
    elev_ft: Optional[List[List[float]]] = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    airspace_cost: Optional[List[List[float]]] = None,
    smooth_airspace_cost: Optional[List[List[float]]] = None,
) -> List[tuple[int, int]]:
    """Greedy line-of-sight shortcutting ('string-pulling') on an A* path.

    For each waypoint, try to skip ahead to the furthest visible waypoint.
    This removes unnecessary grid-aligned zig-zags, producing shorter and
    more natural-looking paths.
    """
    if len(path) <= 2:
        return path

    smoothed = [path[0]]
    i = 0
    while i < len(path) - 1:
        # Try the furthest skip first (greedy); fall back to smaller skips
        best_j = i + 1
        for j in range(len(path) - 1, i + 1, -1):
            if _line_of_sight(grid, passable, path[i], path[j],
                              elev_ft, max_climb_fpm, max_descent_fpm,
                              cruise_kt, climb_speed_kt, descent_speed_kt,
                              airspace_cost, smooth_airspace_cost):
                best_j = j
                break
        smoothed.append(path[best_j])
        i = best_j

    return smoothed


def densify_path(grid: GridSpec, path: List[tuple[int, int]],
                 max_step_nm: float = 2.0) -> List[tuple[int, int]]:
    """Interpolate extra grid points along smoothed path segments.

    After smoothing, segments can span many NM with no intermediate points.
    This re-adds points at roughly `max_step_nm` intervals so that the
    resulting lat/lon path has enough resolution for profile rendering and
    airspace intersection testing.
    """
    if len(path) <= 1:
        return path

    result = [path[0]]
    for k in range(len(path) - 1):
        i0, j0 = path[k]
        i1, j1 = path[k + 1]
        lat0, lon0 = grid.idx_to_latlon(i0, j0)
        lat1, lon1 = grid.idx_to_latlon(i1, j1)
        seg_nm = _haversine_nm(lat0, lon0, lat1, lon1)
        n_sub = max(1, int(seg_nm / max_step_nm))
        for s in range(1, n_sub):
            t = s / n_sub
            ii = int(round(i0 + (i1 - i0) * t))
            jj = int(round(j0 + (j1 - j0) * t))
            ii = max(0, min(grid.n_lat - 1, ii))
            jj = max(0, min(grid.n_lon - 1, jj))
            result.append((ii, jj))
        result.append(path[k + 1])

    return result


def astar_path_streaming(
    grid: GridSpec,
    passable: List[List[bool]],
    start: tuple[int, int],
    goal: tuple[int, int],
    yield_every: int = 100,
    elev_ft: Optional[List[List[float]]] = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
    airspace_cost: Optional[List[List[float]]] = None,
    smooth_airspace_cost: Optional[List[List[float]]] = None,
    time_budget_sec: float = 0.0,
) -> Generator[dict, None, None]:
    """A* that yields progress dicts as it explores.

    Fast path: precomputed uniform step lengths (no per-edge haversine),
    flat 1D arrays for gscore/came_from, integer node IDs (no tuple
    hashing), and equirectangular heuristic (admissible for a uniform
    lat/lon grid at these scales).

    When elev_ft + cruise_kt are provided, also enforces climb/descent
    rate limits per edge (terrain-following model).

    airspace_cost: optional 2D grid of additive cost multipliers per cell.

    time_budget_sec: if > 0, bail with no_path after this many seconds.

    Yields:
      {"type": "explore", "cells": [[lat, lon], ...]}   – batch of explored cells
      {"type": "path", "coords": [[lat, lon], ...], "dist_nm": float}  – final path
      {"type": "no_path"}  – search failed
    """
    import time as _time

    n_lat = grid.n_lat
    n_lon = grid.n_lon
    lat0 = grid.lat0
    lon0 = grid.lon0
    dlat = grid.dlat
    dlon = grid.dlon

    # ── Precomputed step lengths (NM) for the 8-connected uniform grid ──
    # Grid is small enough that haversine at the midpoint is accurate.
    mid_lat = lat0 + (n_lat * 0.5) * dlat
    step_ns = _haversine_nm(mid_lat, 0.0, mid_lat + dlat, 0.0)
    step_ew = _haversine_nm(mid_lat, 0.0, mid_lat, dlon)
    step_diag = math.hypot(step_ns, step_ew)

    # ── Cheap admissible heuristic (equirectangular in NM) ──
    _NM_PER_DEG = 60.0
    goal_i, goal_j = goal
    goal_lat = lat0 + goal_i * dlat
    goal_lon = lon0 + goal_j * dlon
    _cos_goal = math.cos(math.radians(goal_lat))

    def h_nm(node_i: int, node_j: int) -> float:
        lat = lat0 + node_i * dlat
        lon = lon0 + node_j * dlon
        # Use the smaller of the two cosines so the heuristic underestimates
        # (admissibility). At helicopter-mission latitudes both are close.
        cos_lat = math.cos(math.radians(lat))
        cos_scale = cos_lat if cos_lat < _cos_goal else _cos_goal
        dphi = lat - goal_lat
        dlam = (lon - goal_lon) * cos_scale
        return math.sqrt(dphi * dphi + dlam * dlam) * _NM_PER_DEG

    # ── Flat state arrays (indexed by node = i * n_lon + j) ──
    total_cells = n_lat * n_lon
    INF = float("inf")
    gscore = [INF] * total_cells
    came_from = [-1] * total_cells

    start_node = start[0] * n_lon + start[1]
    goal_node = goal_i * n_lon + goal_j
    gscore[start_node] = 0.0

    open_heap: list[tuple[float, float, int]] = []
    heapq.heappush(open_heap, (h_nm(start[0], start[1]), 0.0, start_node))

    # Neighbor deltas: (di, dj, precomputed edge length in NM)
    NEIGHBORS = (
        (-1, -1, step_diag), (-1, 0, step_ns), (-1, 1, step_diag),
        (0, -1, step_ew),                       (0, 1, step_ew),
        (1, -1, step_diag),  (1, 0, step_ns),   (1, 1, step_diag),
    )

    batch_nodes: list[int] = []
    found = False
    _t_start = _time.monotonic() if time_budget_sec > 0 else 0.0
    _budget_check_every = 500  # cheap counter, don't call monotonic every pop
    _pops = 0
    # Bind hot names to locals for speed
    _heappop = heapq.heappop
    _heappush = heapq.heappush
    _has_terrain = elev_ft is not None and cruise_kt > 0
    _has_climb = _has_terrain and max_climb_fpm > 0
    _has_descent = _has_terrain and max_descent_fpm > 0
    _has_airspace = airspace_cost is not None
    _climb_spd = climb_speed_kt if climb_speed_kt > 0 else cruise_kt
    _desc_spd = descent_speed_kt if descent_speed_kt > 0 else cruise_kt

    while open_heap:
        f, g, cur = _heappop(open_heap)
        # Stale entry from a superseded push
        if g > gscore[cur]:
            continue

        if cur == goal_node:
            # Flush any pending batch
            if batch_nodes:
                _step = max(1, len(batch_nodes) // 40)
                cells_out = []
                for k in range(0, len(batch_nodes), _step):
                    node = batch_nodes[k]
                    _i, _j = divmod(node, n_lon)
                    cells_out.append([lat0 + _i * dlat, lon0 + _j * dlon])
                yield {"type": "explore", "cells": cells_out}
            # Reconstruct path (nodes) then convert to (i, j) tuples
            path_nodes = [cur]
            c = cur
            while came_from[c] >= 0:
                c = came_from[c]
                path_nodes.append(c)
            path_nodes.reverse()
            path_idx = [divmod(n, n_lon) for n in path_nodes]
            # Smooth out grid-aligned zig-zags via line-of-sight shortcutting
            path_idx = smooth_path(grid, passable, path_idx,
                                   elev_ft=elev_ft,
                                   max_climb_fpm=max_climb_fpm,
                                   max_descent_fpm=max_descent_fpm,
                                   cruise_kt=cruise_kt,
                                   climb_speed_kt=climb_speed_kt,
                                   descent_speed_kt=descent_speed_kt,
                                   airspace_cost=airspace_cost,
                                   smooth_airspace_cost=smooth_airspace_cost)
            path_idx = densify_path(grid, path_idx)
            coords = [list(grid.idx_to_latlon(i, j)) for i, j in path_idx]
            yield {"type": "path", "coords": coords, "dist_nm": path_nm(grid, path_idx)}
            found = True
            return

        ci, cj = divmod(cur, n_lon)
        batch_nodes.append(cur)
        if len(batch_nodes) >= yield_every:
            # Downsample for the animation dot layer (visual only)
            _step = max(1, len(batch_nodes) // 40)
            cells_out = []
            for k in range(0, len(batch_nodes), _step):
                node = batch_nodes[k]
                _i, _j = divmod(node, n_lon)
                cells_out.append([lat0 + _i * dlat, lon0 + _j * dlon])
            yield {"type": "explore", "cells": cells_out}
            batch_nodes = []

        _pops += 1
        if time_budget_sec > 0 and _pops >= _budget_check_every:
            _pops = 0
            if _time.monotonic() - _t_start > time_budget_sec:
                # Bail: emit no_path with a hint the caller can surface
                yield {"type": "no_path", "reason": "time_budget",
                       "detail": f"Leg exceeded {time_budget_sec:.0f}s search budget."}
                return

        # Row references let us index one dim instead of two
        row_cur = ci
        elev_cur = elev_ft[ci][cj] if _has_terrain else 0.0

        for di, dj, edge_nm in NEIGHBORS:
            ni = ci + di
            nj = cj + dj
            if ni < 0 or nj < 0 or ni >= n_lat or nj >= n_lon:
                continue
            if not passable[ni][nj]:
                continue
            step = edge_nm
            if _has_terrain:
                d_elev = elev_ft[ni][nj] - elev_cur
                if d_elev > 0.0 and _has_climb:
                    time_min = (step / _climb_spd) * 60.0
                    if d_elev / time_min > max_climb_fpm:
                        continue
                elif d_elev < 0.0 and _has_descent:
                    time_min = (step / _desc_spd) * 60.0
                    if (-d_elev) / time_min > max_descent_fpm:
                        continue
            if _has_airspace:
                step *= (1.0 + airspace_cost[ni][nj])
            ng = g + step
            nxt = ni * n_lon + nj
            if ng < gscore[nxt]:
                came_from[nxt] = cur
                gscore[nxt] = ng
                _heappush(open_heap, (ng + h_nm(ni, nj), ng, nxt))

    # Flush residual batch even on failure
    if batch_nodes:
        _step = max(1, len(batch_nodes) // 40)
        cells_out = []
        for k in range(0, len(batch_nodes), _step):
            node = batch_nodes[k]
            _i, _j = divmod(node, n_lon)
            cells_out.append([lat0 + _i * dlat, lon0 + _j * dlon])
        yield {"type": "explore", "cells": cells_out}
    if not found:
        yield {"type": "no_path"}
