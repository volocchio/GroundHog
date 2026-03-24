"""
Persistent route cache backed by SQLite.

Stores computed A* terrain-avoidance legs so identical requests return instantly.
Also tracks route-request history for background precomputation.
"""

import hashlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DB_PATH = Path(__file__).parent / "route_cache.sqlite"

_local = threading.local()


def _conn() -> sqlite3.Connection:
    """One connection per thread (sqlite3 requires this)."""
    c = getattr(_local, "conn", None)
    if c is None:
        c = sqlite3.connect(str(DB_PATH), timeout=10)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        _local.conn = c
    return c


def init_db() -> None:
    """Create tables if they don't exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS leg_cache (
            cache_key   TEXT PRIMARY KEY,
            from_icao   TEXT NOT NULL,
            to_icao     TEXT NOT NULL,
            max_msl_ft  REAL NOT NULL,
            min_agl_ft  REAL NOT NULL,
            detour_fac  REAL NOT NULL,
            dist_nm     REAL,
            path_json   TEXT,
            failed      INTEGER NOT NULL DEFAULT 0,
            created_at  REAL NOT NULL,
            hit_count   INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_leg_airports
            ON leg_cache(from_icao, to_icao);

        CREATE TABLE IF NOT EXISTS route_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            dep_icao    TEXT NOT NULL,
            arr_icao    TEXT NOT NULL,
            max_msl_ft  REAL NOT NULL,
            min_agl_ft  REAL NOT NULL,
            detour_fac  REAL NOT NULL,
            cruise_kt   REAL NOT NULL DEFAULT 120,
            fuel_gal    REAL NOT NULL DEFAULT 48,
            burn_gph    REAL NOT NULL DEFAULT 13,
            reserve_min REAL NOT NULL DEFAULT 30,
            fuel_type   TEXT NOT NULL DEFAULT '100LL',
            max_climb_fpm  REAL NOT NULL DEFAULT 0,
            max_descent_fpm REAL NOT NULL DEFAULT 0,
            climb_speed_kt  REAL NOT NULL DEFAULT 0,
            descent_speed_kt REAL NOT NULL DEFAULT 0,
            requested_at REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_history_route
            ON route_history(dep_icao, arr_icao);
    """)
    c.commit()

    # Migrate existing DBs: add fuel columns to route_history if missing
    existing = {r[1] for r in c.execute("PRAGMA table_info(route_history)").fetchall()}
    migrations = [
        ("cruise_kt",   "REAL NOT NULL DEFAULT 120"),
        ("fuel_gal",    "REAL NOT NULL DEFAULT 48"),
        ("burn_gph",    "REAL NOT NULL DEFAULT 13"),
        ("reserve_min", "REAL NOT NULL DEFAULT 30"),
        ("fuel_type",   "TEXT NOT NULL DEFAULT '100LL'"),
        ("max_climb_fpm",  "REAL NOT NULL DEFAULT 0"),
        ("max_descent_fpm", "REAL NOT NULL DEFAULT 0"),
        ("climb_speed_kt",  "REAL NOT NULL DEFAULT 0"),
        ("descent_speed_kt", "REAL NOT NULL DEFAULT 0"),
    ]
    for col, typedef in migrations:
        if col not in existing:
            c.execute(f"ALTER TABLE route_history ADD COLUMN {col} {typedef}")
    c.commit()


# ── cache key ────────────────────────────────────────────────────────

def _leg_key(from_icao: str, to_icao: str,
             max_msl_ft: float, min_agl_ft: float,
             detour_fac: float,
             max_climb_fpm: float = 0,
             max_descent_fpm: float = 0,
             climb_speed_kt: float = 0,
             descent_speed_kt: float = 0,
             avoidance_tag: str = "") -> str:
    """Deterministic cache key for a single A* leg."""
    raw = f"{from_icao}|{to_icao}|{max_msl_ft:.0f}|{min_agl_ft:.0f}|{detour_fac:.2f}"
    if max_climb_fpm > 0 or max_descent_fpm > 0:
        raw += f"|C{max_climb_fpm:.0f}|D{max_descent_fpm:.0f}"
    if climb_speed_kt > 0 or descent_speed_kt > 0:
        raw += f"|Vc{climb_speed_kt:.0f}|Vd{descent_speed_kt:.0f}"
    if avoidance_tag:
        raw += f"|A{avoidance_tag}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ── read / write ─────────────────────────────────────────────────────

@dataclass
class CachedLeg:
    dist_nm: float
    path: List[Tuple[float, float]]  # [(lat, lon), ...]


def get_leg(from_icao: str, to_icao: str,
            max_msl_ft: float, min_agl_ft: float,
            detour_fac: float,
            max_climb_fpm: float = 0,
            max_descent_fpm: float = 0,
            climb_speed_kt: float = 0,
            descent_speed_kt: float = 0,
            avoidance_tag: str = "") -> Optional[CachedLeg]:
    """Return cached leg or None (also returns None for legs marked failed)."""
    key = _leg_key(from_icao, to_icao, max_msl_ft, min_agl_ft, detour_fac,
                   max_climb_fpm, max_descent_fpm, climb_speed_kt, descent_speed_kt,
                   avoidance_tag)
    c = _conn()
    row = c.execute(
        "SELECT dist_nm, path_json, failed FROM leg_cache WHERE cache_key = ?",
        (key,)
    ).fetchone()
    if row is None:
        return None
    c.execute("UPDATE leg_cache SET hit_count = hit_count + 1 WHERE cache_key = ?", (key,))
    c.commit()
    if row[2]:  # failed
        return CachedLeg(dist_nm=float("inf"), path=[])
    return CachedLeg(dist_nm=row[0], path=json.loads(row[1]))


def is_known_failure(from_icao: str, to_icao: str,
                     max_msl_ft: float, min_agl_ft: float,
                     detour_fac: float,
                     max_climb_fpm: float = 0,
                     max_descent_fpm: float = 0,
                     climb_speed_kt: float = 0,
                     descent_speed_kt: float = 0,
                     avoidance_tag: str = "") -> bool:
    """Quick check: is this leg known to be impossible? (No hit-count bump.)"""
    key = _leg_key(from_icao, to_icao, max_msl_ft, min_agl_ft, detour_fac,
                   max_climb_fpm, max_descent_fpm, climb_speed_kt, descent_speed_kt,
                   avoidance_tag)
    c = _conn()
    row = c.execute(
        "SELECT failed FROM leg_cache WHERE cache_key = ?", (key,)
    ).fetchone()
    return row is not None and row[0] == 1


def put_leg(from_icao: str, to_icao: str,
            max_msl_ft: float, min_agl_ft: float,
            detour_fac: float,
            dist_nm: Optional[float],
            path: Optional[List[Tuple[float, float]]],
            max_climb_fpm: float = 0,
            max_descent_fpm: float = 0,
            climb_speed_kt: float = 0,
            descent_speed_kt: float = 0,
            avoidance_tag: str = "") -> None:
    """Store a computed leg (or a failure marker when dist_nm is None)."""
    key = _leg_key(from_icao, to_icao, max_msl_ft, min_agl_ft, detour_fac,
                   max_climb_fpm, max_descent_fpm, climb_speed_kt, descent_speed_kt,
                   avoidance_tag)
    failed = 1 if dist_nm is None else 0
    c = _conn()
    c.execute("""
        INSERT OR REPLACE INTO leg_cache
            (cache_key, from_icao, to_icao, max_msl_ft, min_agl_ft, detour_fac,
             dist_nm, path_json, failed, created_at, hit_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    """, (
        key, from_icao, to_icao, max_msl_ft, min_agl_ft, detour_fac,
        dist_nm, json.dumps(path) if path else None,
        failed, time.time(),
    ))
    c.commit()


# ── history ──────────────────────────────────────────────────────────

def record_request(dep_icao: str, arr_icao: str,
                   max_msl_ft: float, min_agl_ft: float,
                   detour_fac: float,
                   cruise_kt: float = 120, fuel_gal: float = 48,
                   burn_gph: float = 13, reserve_min: float = 30,
                   fuel_type: str = "100LL",
                   max_climb_fpm: float = 0,
                   max_descent_fpm: float = 0,
                   climb_speed_kt: float = 0,
                   descent_speed_kt: float = 0) -> None:
    """Log a route request for frequency analysis."""
    c = _conn()
    c.execute("""
        INSERT INTO route_history
            (dep_icao, arr_icao, max_msl_ft, min_agl_ft, detour_fac,
             cruise_kt, fuel_gal, burn_gph, reserve_min, fuel_type,
             max_climb_fpm, max_descent_fpm,
             climb_speed_kt, descent_speed_kt, requested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (dep_icao, arr_icao, max_msl_ft, min_agl_ft, detour_fac,
          cruise_kt, fuel_gal, burn_gph, reserve_min, fuel_type,
          max_climb_fpm, max_descent_fpm,
          climb_speed_kt, descent_speed_kt, time.time()))
    c.commit()


def popular_routes(limit: int = 20) -> List[Dict]:
    """Return the most-requested (dep, arr, msl, agl, detour, fuel, perf) combos."""
    c = _conn()
    rows = c.execute("""
        SELECT dep_icao, arr_icao, max_msl_ft, min_agl_ft, detour_fac,
               cruise_kt, fuel_gal, burn_gph, reserve_min, fuel_type,
               max_climb_fpm, max_descent_fpm,
               climb_speed_kt, descent_speed_kt,
               COUNT(*) as cnt, MAX(requested_at) as last_req
        FROM route_history
        GROUP BY dep_icao, arr_icao, max_msl_ft, min_agl_ft, detour_fac,
                 cruise_kt, fuel_gal, burn_gph, reserve_min, fuel_type,
                 max_climb_fpm, max_descent_fpm,
                 climb_speed_kt, descent_speed_kt
        ORDER BY cnt DESC, last_req DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [
        {"dep": r[0], "arr": r[1], "max_msl_ft": r[2], "min_agl_ft": r[3],
         "detour_fac": r[4], "cruise_kt": r[5], "fuel_gal": r[6],
         "burn_gph": r[7], "reserve_min": r[8], "fuel_type": r[9],
         "max_climb_fpm": r[10], "max_descent_fpm": r[11],
         "climb_speed_kt": r[12], "descent_speed_kt": r[13],
         "count": r[14], "last_requested": r[15]}
        for r in rows
    ]


# ── stats ────────────────────────────────────────────────────────────

def cache_stats() -> Dict:
    """Return summary of cache state."""
    c = _conn()
    total = c.execute("SELECT COUNT(*) FROM leg_cache").fetchone()[0]
    hits = c.execute("SELECT SUM(hit_count) FROM leg_cache").fetchone()[0] or 0
    failed = c.execute("SELECT COUNT(*) FROM leg_cache WHERE failed = 1").fetchone()[0]
    routes_logged = c.execute("SELECT COUNT(*) FROM route_history").fetchone()[0]
    return {
        "cached_legs": total,
        "total_hits": hits,
        "failed_legs": failed,
        "routes_logged": routes_logged,
    }
