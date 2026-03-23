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
) -> Optional[List[tuple[int, int]]]:
    """8-connected A* over passable grid.

    When elev_ft + cruise_kt are provided, also enforces climb/descent rate
    limits per edge (terrain-following model).  climb_speed_kt / descent_speed_kt
    override cruise_kt for ascending / descending edges respectively.
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


def astar_path_streaming(
    grid: GridSpec,
    passable: List[List[bool]],
    start: tuple[int, int],
    goal: tuple[int, int],
    yield_every: int = 20,
    elev_ft: Optional[List[List[float]]] = None,
    max_climb_fpm: float = 0,
    max_descent_fpm: float = 0,
    cruise_kt: float = 0,
    climb_speed_kt: float = 0,
    descent_speed_kt: float = 0,
) -> Generator[dict, None, None]:
    """A* that yields progress dicts as it explores.

    When elev_ft + cruise_kt are provided, also enforces climb/descent rate
    limits per edge (terrain-following model).

    Yields:
      {"type": "explore", "cells": [[lat, lon], ...]}   – batch of explored cells
      {"type": "path", "coords": [[lat, lon], ...], "dist_nm": float}  – final path
      {"type": "no_path"}  – search failed
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

    batch: list[list[float]] = []
    found = False

    while open_heap:
        f, g, cur = heapq.heappop(open_heap)
        if cur == goal:
            # flush remaining batch
            if batch:
                yield {"type": "explore", "cells": batch}
            # reconstruct path
            path_idx = [cur]
            c = cur
            while c in came_from:
                c = came_from[c]
                path_idx.append(c)
            path_idx.reverse()
            coords = [list(grid.idx_to_latlon(i, j)) for i, j in path_idx]
            yield {"type": "path", "coords": coords, "dist_nm": path_nm(grid, path_idx)}
            found = True
            return

        lat, lon = grid.idx_to_latlon(*cur)
        batch.append([lat, lon])
        if len(batch) >= yield_every:
            yield {"type": "explore", "cells": batch}
            batch = []

        ci, cj = cur
        for di, dj in neighbors:
            ni, nj = ci + di, cj + dj
            if ni < 0 or nj < 0 or ni >= grid.n_lat or nj >= grid.n_lon:
                continue
            if not passable[ni][nj]:
                continue
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
            ng = gscore[cur] + step
            nxt = (ni, nj)
            if ng < gscore.get(nxt, float("inf")):
                came_from[nxt] = cur
                gscore[nxt] = ng
                heapq.heappush(open_heap, (ng + h(nxt, goal), ng, nxt))

    if batch:
        yield {"type": "explore", "cells": batch}
    if not found:
        yield {"type": "no_path"}
