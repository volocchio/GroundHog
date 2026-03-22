from __future__ import annotations

import math
import sqlite3
import time
from dataclasses import dataclass
from typing import Iterable, List, Tuple

import requests


@dataclass
class ElevationProvider:
    """MVP elevation provider using OpenTopoData (SRTM90m) with SQLite cache.

    Cache key rounds lat/lon to 5 decimals (~1m) by default; for our 1km grid
    we can round to 3-4 decimals.
    """

    db_path: str
    base_url: str = "https://api.opentopodata.org/v1/srtm90m"
    round_decimals: int = 4
    batch_size: int = 100
    min_delay_s: float = 0.0

    def __post_init__(self):
        self._init_db()

    def _init_db(self):
        con = sqlite3.connect(self.db_path)
        con.execute(
            "CREATE TABLE IF NOT EXISTS elev_cache (lat REAL, lon REAL, elev_m REAL, PRIMARY KEY(lat, lon))"
        )
        con.commit()
        con.close()

    def _round(self, lat: float, lon: float) -> tuple[float, float]:
        return (round(lat, self.round_decimals), round(lon, self.round_decimals))

    def get_many_m(self, points: Iterable[tuple[float, float]]) -> List[float]:
        pts = [self._round(lat, lon) for lat, lon in points]
        if not pts:
            return []

        con = sqlite3.connect(self.db_path)
        out: List[float] = [math.nan] * len(pts)

        # fetch cached
        missing: List[tuple[int, float, float]] = []
        for i, (lat, lon) in enumerate(pts):
            cur = con.execute("SELECT elev_m FROM elev_cache WHERE lat=? AND lon=?", (lat, lon))
            row = cur.fetchone()
            if row is None:
                missing.append((i, lat, lon))
            else:
                out[i] = float(row[0])

        # fetch missing from API in batches
        for j in range(0, len(missing), self.batch_size):
            batch = missing[j : j + self.batch_size]
            locs = "|".join(f"{lat},{lon}" for _, lat, lon in batch)
            url = f"{self.base_url}?locations={locs}"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
            results = data.get("results") or []
            if len(results) != len(batch):
                raise RuntimeError(f"Elevation API batch mismatch: {len(results)} vs {len(batch)}")

            # store
            for (idx, lat, lon), res in zip(batch, results):
                elev_m = res.get("elevation")
                if elev_m is None:
                    elev_m = math.nan
                out[idx] = float(elev_m)
                con.execute(
                    "INSERT OR REPLACE INTO elev_cache(lat, lon, elev_m) VALUES (?,?,?)",
                    (lat, lon, float(elev_m)),
                )
            con.commit()

            if self.min_delay_s:
                time.sleep(self.min_delay_s)

        con.close()
        return out

    def get_one_m(self, lat: float, lon: float) -> float:
        return self.get_many_m([(lat, lon)])[0]


def meters_to_feet(m: float) -> float:
    return m * 3.280839895
