#!/usr/bin/env python3
"""Pre-download SRTM tiles for CONUS so first routes don't wait on S3.

Run once after container start: python3 -m mvp_backend.prefetch_srtm
Covers lat 24-50, lon -125 to -66 (CONUS + buffer).  ~700 tiles, ~1.5 GB.
"""
import os
import sys
import time

from mvp_backend.srtm_local import SRTMProvider

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CACHE = os.path.join(ROOT, "mvp_backend", "srtm_cache")

LAT_MIN, LAT_MAX = 24, 50   # Florida Keys to Canadian border
LON_MIN, LON_MAX = -125, -66  # Pacific coast to Atlantic coast


def main():
    provider = SRTMProvider(cache_dir=CACHE)
    total = (LAT_MAX - LAT_MIN) * (LON_MAX - LON_MIN)
    done = 0
    skipped = 0
    failed = 0
    t0 = time.time()

    for lat in range(LAT_MIN, LAT_MAX):
        for lon in range(LON_MIN, LON_MAX):
            done += 1
            # Check if already cached
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            hgt = os.path.join(CACHE, f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}.hgt")
            if os.path.exists(hgt):
                skipped += 1
                continue
            try:
                provider._load_tile(lat, lon)
                if done % 20 == 0:
                    elapsed = time.time() - t0
                    pct = done / total * 100
                    print(f"  [{pct:5.1f}%] {done}/{total} tiles  "
                          f"({skipped} cached, {failed} failed)  "
                          f"{elapsed:.0f}s elapsed", flush=True)
            except Exception as e:
                failed += 1
                # Ocean tiles return 404 — that's fine
                if "404" not in str(e):
                    print(f"  WARN: tile N{lat} W{abs(lon)} failed: {e}",
                          file=sys.stderr, flush=True)

    elapsed = time.time() - t0
    print(f"\nDone: {done} tiles checked, {skipped} cached, "
          f"{done - skipped - failed} downloaded, {failed} failed (ocean).  "
          f"{elapsed:.0f}s total.", flush=True)


if __name__ == "__main__":
    main()
