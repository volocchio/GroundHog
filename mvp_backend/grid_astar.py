from __future__ import annotations

import heapq
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


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
) -> Optional[List[tuple[int, int]]]:
    """8-connected A* over passable grid."""

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
