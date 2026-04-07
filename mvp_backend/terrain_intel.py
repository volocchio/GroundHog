"""
Terrain Intelligence — precomputed corridor viability data.

Divides terrain into 1°×1° sectors.  For each adjacent sector pair AND each
altitude band, a BFS flood-fill determines whether a connected path exists.

The planner consults this data to:
  1. Instantly reject impossible altitude / route combos.
  2. Find the minimum viable MSL for a given corridor.
  3. Guide fuel-stop selection toward known-passable corridors.

The precomputation runs via  scripts/precompute_terrain.py  and stores results
in  mvp_backend/terrain_intel.sqlite.
"""

from __future__ import annotations

import collections
import json
import math
import os
import sqlite3
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from mvp_backend.srtm_local import SRTMProvider

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(os.path.dirname(__file__), "terrain_intel.sqlite")
SRTM_DIR = os.path.join(ROOT, "mvp_backend", "srtm_cache")

# Altitude bands we precompute (MSL feet).  The system stores whether a
# corridor is passable at each of these ceilings assuming 1 000 ft AGL.
ALTITUDE_BANDS = list(range(500, 13000, 500))

# Default AGL clearance baked into precomputation
DEFAULT_AGL_FT = 1000

# Sector resolution for the BFS grid (km)
SECTOR_CELL_KM = 1.0

_local = threading.local()


def _conn() -> sqlite3.Connection:
    c = getattr(_local, "conn", None)
    if c is None:
        c = sqlite3.connect(DB_PATH, timeout=30)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        _local.conn = c
    return c


def init_db() -> None:
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS sector_stats (
            lat_bin   INTEGER NOT NULL,
            lon_bin   INTEGER NOT NULL,
            min_ft    REAL,
            max_ft    REAL,
            median_ft REAL,
            p25_ft    REAL,
            p75_ft    REAL,
            analyzed_at REAL,
            PRIMARY KEY (lat_bin, lon_bin)
        );

        CREATE TABLE IF NOT EXISTS sector_connectivity (
            from_lat  INTEGER NOT NULL,
            from_lon  INTEGER NOT NULL,
            to_lat    INTEGER NOT NULL,
            to_lon    INTEGER NOT NULL,
            msl_ft    REAL    NOT NULL,
            agl_ft    REAL    NOT NULL,
            viable    INTEGER NOT NULL,
            pass_elev_ft REAL,
            analyzed_at  REAL,
            PRIMARY KEY (from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft)
        );

        CREATE TABLE IF NOT EXISTS corridor_viability (
            from_lat  REAL NOT NULL,
            from_lon  REAL NOT NULL,
            to_lat    REAL NOT NULL,
            to_lon    REAL NOT NULL,
            msl_ft    REAL NOT NULL,
            agl_ft    REAL NOT NULL,
            viable    INTEGER NOT NULL,
            min_viable_msl REAL,
            best_path_json TEXT,
            analyzed_at REAL,
            PRIMARY KEY (from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft)
        );

        CREATE INDEX IF NOT EXISTS idx_conn_msl
            ON sector_connectivity(msl_ft, viable);
    """)
    c.commit()


# ── helpers ──────────────────────────────────────────────────────────

def _m_to_ft(m: float) -> float:
    return m * 3.28084


def _deg_per_km_lat() -> float:
    return 1.0 / 110.574


def _deg_per_km_lon(lat: float) -> float:
    return 1.0 / (111.320 * math.cos(math.radians(lat)) + 1e-9)


# ── sector stats ─────────────────────────────────────────────────────

def analyze_sector(provider: SRTMProvider, lat_bin: int, lon_bin: int) -> dict:
    """Sample terrain in a 1°×1° sector and store stats."""
    step = SECTOR_CELL_KM
    dlat = step * _deg_per_km_lat()
    dlon = step * _deg_per_km_lon(lat_bin + 0.5)

    points = []
    lat = lat_bin + dlat / 2
    while lat < lat_bin + 1.0:
        lon = lon_bin + dlon / 2
        while lon < lon_bin + 1.0:
            points.append((lat, lon))
            lon += dlon
        lat += dlat

    if not points:
        return {}

    elevs_m = provider.get_many_m(points)
    elevs_ft = sorted(_m_to_ft(m) for m in elevs_m if m == m)  # skip NaN
    if not elevs_ft:
        return {}

    n = len(elevs_ft)
    stats = {
        "min_ft": elevs_ft[0],
        "max_ft": elevs_ft[-1],
        "median_ft": elevs_ft[n // 2],
        "p25_ft": elevs_ft[n // 4],
        "p75_ft": elevs_ft[3 * n // 4],
    }

    c = _conn()
    c.execute("""
        INSERT OR REPLACE INTO sector_stats
            (lat_bin, lon_bin, min_ft, max_ft, median_ft, p25_ft, p75_ft, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (lat_bin, lon_bin, stats["min_ft"], stats["max_ft"],
          stats["median_ft"], stats["p25_ft"], stats["p75_ft"], time.time()))
    c.commit()
    return stats


def get_sector_stats(lat_bin: int, lon_bin: int) -> Optional[dict]:
    c = _conn()
    row = c.execute(
        "SELECT min_ft, max_ft, median_ft, p25_ft, p75_ft FROM sector_stats WHERE lat_bin=? AND lon_bin=?",
        (lat_bin, lon_bin)
    ).fetchone()
    if not row:
        return None
    return {"min_ft": row[0], "max_ft": row[1], "median_ft": row[2],
            "p25_ft": row[3], "p75_ft": row[4]}


# ── sector connectivity (BFS flood-fill) ────────────────────────────

def _build_sector_grid(provider: SRTMProvider, lat_bin: int, lon_bin: int,
                       cell_km: float = SECTOR_CELL_KM) -> Tuple[list, int, int, float, float]:
    """Build a 2D elevation grid for a 1°×1° sector.

    Returns (elev_ft_flat, n_rows, n_cols, dlat, dlon).
    """
    mid_lat = lat_bin + 0.5
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    n_rows = int(math.ceil(1.0 / dlat))
    n_cols = int(math.ceil(1.0 / dlon))

    points = []
    for r in range(n_rows):
        for c_ in range(n_cols):
            lat = lat_bin + (r + 0.5) * dlat
            lon = lon_bin + (c_ + 0.5) * dlon
            points.append((lat, lon))

    elevs_m = provider.get_many_m(points)
    elev_ft = [_m_to_ft(m) if m == m else float("inf") for m in elevs_m]
    return elev_ft, n_rows, n_cols, dlat, dlon


def analyze_connectivity(provider: SRTMProvider,
                         from_lat: int, from_lon: int,
                         to_lat: int, to_lon: int,
                         msl_ft: float, agl_ft: float = DEFAULT_AGL_FT,
                         cell_km: float = 2.0) -> bool:
    """BFS flood-fill to test if you can traverse from one sector to an
    adjacent sector at the given MSL / AGL.

    Builds a combined grid spanning both sectors, marks cells passable if
    terrain ≤ (msl_ft - agl_ft), then flood-fills from the 'entry' border
    to see if the 'exit' border is reachable.

    Uses 2 km cells by default for fast precomputation (sufficient to detect
    passable corridors — the fine-grained 1 km A* runs at planning time).
    """
    ceiling_ft = msl_ft - agl_ft

    # Determine direction
    dlat_dir = to_lat - from_lat  # +1 = northward, -1 = southward, 0 = E/W
    dlon_dir = to_lon - from_lon  # +1 = eastward, -1 = westward, 0 = N/S

    # Build combined grid of the two sectors
    min_lat_bin = min(from_lat, to_lat)
    min_lon_bin = min(from_lon, to_lon)
    span_lat = abs(dlat_dir) + 1  # 1 or 2 degrees
    span_lon = abs(dlon_dir) + 1

    mid_lat = min_lat_bin + span_lat * 0.5
    dlat = cell_km * _deg_per_km_lat()
    dlon = cell_km * _deg_per_km_lon(mid_lat)

    n_rows = int(math.ceil(span_lat / dlat))
    n_cols = int(math.ceil(span_lon / dlon))

    # Sample terrain
    points = []
    for r in range(n_rows):
        for c_ in range(n_cols):
            lat = min_lat_bin + (r + 0.5) * dlat
            lon = min_lon_bin + (c_ + 0.5) * dlon
            points.append((lat, lon))

    elevs_m = provider.get_many_m(points)

    # Build passable grid
    passable = [False] * (n_rows * n_cols)
    max_pass_elev = 0.0
    for idx, m in enumerate(elevs_m):
        ft = _m_to_ft(m) if m == m else float("inf")
        if ft <= ceiling_ft:
            passable[idx] = True

    # Determine entry and exit borders
    # "from" sector occupies certain rows/cols; entry border is the far edge
    # of "from" sector; exit border is the far edge of "to" sector.
    from_row_start = int(round((from_lat - min_lat_bin) / dlat))
    from_row_end = int(round((from_lat + 1 - min_lat_bin) / dlat))
    from_col_start = int(round((from_lon - min_lon_bin) / dlon))
    from_col_end = int(round((from_lon + 1 - min_lon_bin) / dlon))

    to_row_start = int(round((to_lat - min_lat_bin) / dlat))
    to_row_end = int(round((to_lat + 1 - min_lat_bin) / dlat))
    to_col_start = int(round((to_lon - min_lon_bin) / dlon))
    to_col_end = int(round((to_lon + 1 - min_lon_bin) / dlon))

    # Entry cells: edge of 'from' sector closest to 'to' sector
    entry_cells = set()
    exit_cells = set()

    if dlat_dir != 0:
        # N-S transition: entry is the row boundary between sectors
        if dlat_dir > 0:  # going north
            border_row = from_row_end - 1
            exit_row = min(to_row_end - 1, n_rows - 1)
        else:  # going south
            border_row = from_row_start
            exit_row = max(to_row_start, 0)
        for c_ in range(n_cols):
            entry_cells.add(border_row * n_cols + c_)
            exit_cells.add(exit_row * n_cols + c_)

    if dlon_dir != 0:
        # E-W transition: entry is the col boundary between sectors
        if dlon_dir > 0:  # going east
            border_col = from_col_end - 1
            exit_col = min(to_col_end - 1, n_cols - 1)
        else:  # going west
            border_col = from_col_start
            exit_col = max(to_col_start, 0)
        for r in range(n_rows):
            entry_cells.add(r * n_cols + border_col)
            exit_cells.add(r * n_cols + exit_col)

    # Diagonal: combine both borders
    if dlat_dir != 0 and dlon_dir != 0:
        # already added both; entry/exit are corner regions
        pass

    # BFS from all passable entry cells
    queue = collections.deque()
    visited = set()
    for idx in entry_cells:
        if 0 <= idx < len(passable) and passable[idx]:
            queue.append(idx)
            visited.add(idx)

    neighbors_8 = [(-1, -1), (-1, 0), (-1, 1),
                    (0, -1),           (0, 1),
                    (1, -1),  (1, 0),  (1, 1)]

    best_pass_elev = float("inf")
    viable = False

    while queue:
        idx = queue.popleft()
        if idx in exit_cells:
            viable = True
            # Track the elevation at the exit for pass info
            m = elevs_m[idx]
            ft = _m_to_ft(m) if m == m else float("inf")
            best_pass_elev = min(best_pass_elev, ft)
            # Don't break — find lowest exit point

        r = idx // n_cols
        c_ = idx % n_cols
        for dr, dc in neighbors_8:
            nr, nc = r + dr, c_ + dc
            if 0 <= nr < n_rows and 0 <= nc < n_cols:
                nidx = nr * n_cols + nc
                if nidx not in visited and passable[nidx]:
                    visited.add(nidx)
                    queue.append(nidx)

    # Store result
    pass_elev = best_pass_elev if viable else None
    c = _conn()
    c.execute("""
        INSERT OR REPLACE INTO sector_connectivity
            (from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft, viable, pass_elev_ft, analyzed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft,
          1 if viable else 0, pass_elev, time.time()))
    c.commit()
    return viable


def get_connectivity(from_lat: int, from_lon: int,
                     to_lat: int, to_lon: int,
                     msl_ft: float, agl_ft: float = DEFAULT_AGL_FT) -> Optional[bool]:
    """Check cached connectivity. Returns None if not yet analyzed."""
    c = _conn()
    row = c.execute(
        "SELECT viable FROM sector_connectivity WHERE from_lat=? AND from_lon=? AND to_lat=? AND to_lon=? AND msl_ft=? AND agl_ft=?",
        (from_lat, from_lon, to_lat, to_lon, msl_ft, agl_ft)
    ).fetchone()
    if row is None:
        return None
    return bool(row[0])


# ── corridor viability (minimax A* over sector graph) ────────────────

def find_min_viable_msl(from_lat: float, from_lon: float,
                        to_lat: float, to_lon: float,
                        agl_ft: float = DEFAULT_AGL_FT) -> Optional[float]:
    """Find the minimum MSL altitude that allows transit from (from_lat,lon)
    to (to_lat,lon) using precomputed sector connectivity.

    Uses a binary-search-like approach over altitude bands, running minimax
    A* at each band.  Returns None if no band is viable or if the sectors
    haven't been analyzed yet.
    """
    for msl in ALTITUDE_BANDS:
        path = _sector_astar(from_lat, from_lon, to_lat, to_lon, msl, agl_ft)
        if path is not None:
            return float(msl)
    return None


def check_viability(from_lat: float, from_lon: float,
                    to_lat: float, to_lon: float,
                    msl_ft: float, agl_ft: float = DEFAULT_AGL_FT) -> dict:
    """Quick viability check for a corridor.

    Returns:
      {"viable": True/False,
       "min_viable_msl": float or None,
       "analyzed": True/False (whether sector data exists),
       "explanation": str}
    """
    # Snap to altitude band (round up to nearest band)
    check_msl = msl_ft
    for band in ALTITUDE_BANDS:
        if band >= msl_ft:
            check_msl = band
            break

    path = _sector_astar(from_lat, from_lon, to_lat, to_lon, check_msl, agl_ft)

    if path is None:
        # Check if we have ANY sector data
        from_bin = (math.floor(from_lat), math.floor(from_lon))
        to_bin = (math.floor(to_lat), math.floor(to_lon))
        c = _conn()
        count = c.execute("SELECT COUNT(*) FROM sector_connectivity WHERE msl_ft=?",
                          (check_msl,)).fetchone()[0]
        if count == 0:
            return {"viable": None, "min_viable_msl": None, "analyzed": False,
                    "explanation": "Terrain intelligence not yet computed. Run precompute_terrain.py."}

        # Find min viable MSL
        min_msl = find_min_viable_msl(from_lat, from_lon, to_lat, to_lon, agl_ft)
        if min_msl is not None:
            return {"viable": False, "min_viable_msl": min_msl, "analyzed": True,
                    "explanation": f"Not viable at {msl_ft:.0f}' MSL. Minimum viable: {min_msl:.0f}' MSL."}
        return {"viable": False, "min_viable_msl": None, "analyzed": True,
                "explanation": f"No viable corridor found at any precomputed altitude up to {ALTITUDE_BANDS[-1]}' MSL."}

    return {"viable": True, "min_viable_msl": msl_ft, "analyzed": True,
            "explanation": f"Corridor viable at {msl_ft:.0f}' MSL.",
            "sector_path": path}


def _sector_astar(from_lat: float, from_lon: float,
                  to_lat: float, to_lon: float,
                  msl_ft: float, agl_ft: float) -> Optional[List[Tuple[int, int]]]:
    """A* over sector grid at a specific MSL.

    Returns list of (lat_bin, lon_bin) sectors forming the path, or None.
    """
    import heapq

    start = (math.floor(from_lat), math.floor(from_lon))
    goal = (math.floor(to_lat), math.floor(to_lon))

    if start == goal:
        return [start]

    # Snap MSL to nearest band for DB lookup
    lookup_msl = msl_ft
    for band in ALTITUDE_BANDS:
        if band >= msl_ft:
            lookup_msl = band
            break

    # Load all connectivity data for this altitude band into memory
    c = _conn()
    rows = c.execute(
        "SELECT from_lat, from_lon, to_lat, to_lon, viable FROM sector_connectivity WHERE msl_ft=? AND agl_ft=?",
        (lookup_msl, agl_ft)
    ).fetchall()

    if not rows:
        return None

    # Build adjacency: (from_lat, from_lon) → set of (to_lat, to_lon) that are viable
    adj: Dict[Tuple[int, int], set] = {}
    for fl, fln, tl, tln, v in rows:
        if v:
            key = (fl, fln)
            if key not in adj:
                adj[key] = set()
            adj[key].add((tl, tln))

    def h(a: Tuple[int, int], b: Tuple[int, int]) -> float:
        return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

    open_heap: list = [(h(start, goal), 0.0, start)]
    gscore = {start: 0.0}
    came_from: Dict = {}

    while open_heap:
        f, g, cur = heapq.heappop(open_heap)
        if cur == goal:
            path = [cur]
            while cur in came_from:
                cur = came_from[cur]
                path.append(cur)
            path.reverse()
            return path

        for nxt in adj.get(cur, set()):
            ng = g + 1.0  # uniform cost per sector
            if ng < gscore.get(nxt, float("inf")):
                gscore[nxt] = ng
                came_from[nxt] = cur
                heapq.heappush(open_heap, (ng + h(nxt, goal), ng, nxt))

    return None


# ── precomputation driver ────────────────────────────────────────────

def precompute_region(lat_range: Tuple[int, int],
                      lon_range: Tuple[int, int],
                      altitude_bands: Optional[List[float]] = None,
                      agl_ft: float = DEFAULT_AGL_FT,
                      progress_cb=None) -> dict:
    """Analyze sectors and connectivity for a rectangular region.

    lat_range: (min_lat, max_lat) inclusive integer degrees
    lon_range: (min_lon, max_lon) inclusive integer degrees
    progress_cb: optional callable(step, total, message)

    Returns summary dict.
    """
    init_db()
    bands = altitude_bands or ALTITUDE_BANDS
    provider = SRTMProvider(cache_dir=SRTM_DIR)

    lat_min, lat_max = lat_range
    lon_min, lon_max = lon_range

    sectors = [(lat, lon) for lat in range(lat_min, lat_max + 1)
               for lon in range(lon_min, lon_max + 1)]

    # Step 1: sector stats
    total_steps = len(sectors) + 0  # connectivity counted below
    step = 0
    stats_computed = 0

    for lat, lon in sectors:
        step += 1
        if progress_cb:
            progress_cb(step, total_steps, f"Sector stats {lat}°N {abs(lon)}°W")
        existing = get_sector_stats(lat, lon)
        if existing is None:
            try:
                analyze_sector(provider, lat, lon)
                stats_computed += 1
            except Exception as e:
                if progress_cb:
                    progress_cb(step, total_steps, f"  SKIP {lat},{lon}: {e}")

    # Step 2: connectivity between adjacent sectors
    adjacencies = []
    dirs_8 = [(-1, -1), (-1, 0), (-1, 1),
              (0, -1),           (0, 1),
              (1, -1),  (1, 0),  (1, 1)]
    sector_set = set(sectors)
    for lat, lon in sectors:
        for dl, dn in dirs_8:
            nb = (lat + dl, lon + dn)
            if nb in sector_set:
                adjacencies.append((lat, lon, nb[0], nb[1]))

    total_conn = len(adjacencies) * len(bands)
    total_steps_all = len(sectors) + total_conn
    conn_computed = 0
    conn_skipped = 0

    for band in bands:
        for fl, fln, tl, tln in adjacencies:
            step += 1
            if progress_cb and step % 50 == 0:
                progress_cb(step, total_steps_all,
                            f"Connectivity {fl}°,{fln}° → {tl}°,{tln}° @ {band}' MSL")

            existing = get_connectivity(fl, fln, tl, tln, band, agl_ft)
            if existing is not None:
                conn_skipped += 1
                continue

            try:
                analyze_connectivity(provider, fl, fln, tl, tln, band, agl_ft)
                conn_computed += 1
            except Exception as e:
                if progress_cb:
                    progress_cb(step, total_steps_all,
                                f"  SKIP {fl},{fln}→{tl},{tln} @ {band}': {e}")

    return {
        "sectors_analyzed": stats_computed,
        "sectors_total": len(sectors),
        "connectivity_computed": conn_computed,
        "connectivity_skipped": conn_skipped,
        "connectivity_total": total_conn,
    }


# ── summary / diagnostic queries ────────────────────────────────────

def coverage_summary() -> dict:
    """Return a summary of what's been precomputed."""
    c = _conn()
    sector_count = c.execute("SELECT COUNT(*) FROM sector_stats").fetchone()[0]
    conn_count = c.execute("SELECT COUNT(*) FROM sector_connectivity").fetchone()[0]
    conn_viable = c.execute("SELECT COUNT(*) FROM sector_connectivity WHERE viable=1").fetchone()[0]
    bands_stored = [r[0] for r in c.execute(
        "SELECT DISTINCT msl_ft FROM sector_connectivity ORDER BY msl_ft").fetchall()]

    lat_range = c.execute("SELECT MIN(lat_bin), MAX(lat_bin) FROM sector_stats").fetchone()
    lon_range = c.execute("SELECT MIN(lon_bin), MAX(lon_bin) FROM sector_stats").fetchone()

    return {
        "sectors": sector_count,
        "connectivity_records": conn_count,
        "connectivity_viable": conn_viable,
        "altitude_bands": bands_stored,
        "lat_range": lat_range if lat_range[0] is not None else None,
        "lon_range": lon_range if lon_range[0] is not None else None,
    }


# Ensure tables exist on import
try:
    init_db()
except Exception:
    pass
