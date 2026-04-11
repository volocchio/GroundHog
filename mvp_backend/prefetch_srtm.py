#!/usr/bin/env python3
"""Pre-download SRTM tiles for CONUS so first routes don't wait on S3.

Run once after container start: python3 -m mvp_backend.prefetch_srtm
Covers lat 24-50, lon -125 to -66 (CONUS + buffer).  ~1500 tiles, ~1.5 GB.
Uses concurrent downloads for speed.  Retries transient failures.
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from mvp_backend.srtm_local import SRTMProvider

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CACHE = os.path.join(ROOT, "mvp_backend", "srtm_cache")

LAT_MIN, LAT_MAX = 24, 50   # Florida Keys to Canadian border
LON_MIN, LON_MAX = -125, -66  # Pacific coast to Atlantic coast

MAX_RETRIES = 3
MAX_WORKERS = 8  # concurrent download threads


def _tile_path(lat: int, lon: int) -> str:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    return os.path.join(CACHE, f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}.hgt")


def _fetch_one(provider: SRTMProvider, lat: int, lon: int) -> str:
    """Download a single tile with retries.  Returns status string."""
    if os.path.exists(_tile_path(lat, lon)):
        return "cached"
    for attempt in range(MAX_RETRIES):
        try:
            provider._load_tile(lat, lon)
            return "downloaded"
        except Exception as e:
            if "404" in str(e) or "403" in str(e):
                return "ocean"  # no tile (ocean / outside coverage)
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # exponential backoff
                continue
            print(f"  WARN: tile ({lat},{lon}) failed after {MAX_RETRIES} attempts: {e}",
                  file=sys.stderr, flush=True)
            return "failed"
    return "failed"


def main():
    os.makedirs(CACHE, exist_ok=True)
    provider = SRTMProvider(cache_dir=CACHE)

    tiles = [(lat, lon)
             for lat in range(LAT_MIN, LAT_MAX)
             for lon in range(LON_MIN, LON_MAX)]
    total = len(tiles)
    cached = downloaded = ocean = failed = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, provider, lat, lon): (lat, lon)
                   for lat, lon in tiles}
        for i, fut in enumerate(as_completed(futures), 1):
            status = fut.result()
            if status == "cached":
                cached += 1
            elif status == "downloaded":
                downloaded += 1
            elif status == "ocean":
                ocean += 1
            else:
                failed += 1
            if i % 50 == 0 or i == total:
                elapsed = time.time() - t0
                pct = i / total * 100
                print(f"  [{pct:5.1f}%] {i}/{total}  "
                      f"{cached} cached, {downloaded} new, "
                      f"{ocean} ocean, {failed} failed  "
                      f"{elapsed:.0f}s", flush=True)

    elapsed = time.time() - t0
    print(f"\nDone: {total} tiles, {cached} cached, "
          f"{downloaded} downloaded, {ocean} ocean, {failed} failed.  "
          f"{elapsed:.0f}s total.", flush=True)


if __name__ == "__main__":
    main()
