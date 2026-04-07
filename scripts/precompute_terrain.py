#!/usr/bin/env python3
"""
Precompute terrain intelligence for GroundHog.

Analyzes sector terrain statistics and adjacent-sector connectivity at
multiple altitude bands.  Results are stored in terrain_intel.sqlite and
used by the planner to instantly reject impossible corridors and suggest
minimum viable MSL altitudes.

Usage:
  # Full western US (takes a while — downloads SRTM tiles as needed):
  python scripts/precompute_terrain.py

  # Specific corridor (KSZT → E60 region):
  python scripts/precompute_terrain.py --lat 32 48 --lon -118 -110

  # Custom altitude bands:
  python scripts/precompute_terrain.py --bands 4000 5000 6000 7000 8000

  # Quick viability check (no precomputation):
  python scripts/precompute_terrain.py --check 48.18 -116.56 32.80 -111.58 --msl 5000

  # Show coverage summary:
  python scripts/precompute_terrain.py --summary
"""
from __future__ import annotations

import argparse
import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mvp_backend import terrain_intel


def _progress(step: int, total: int, msg: str):
    pct = 100 * step / max(total, 1)
    print(f"\r  [{pct:5.1f}%] {msg}".ljust(80), end="", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Precompute terrain intelligence")
    parser.add_argument("--lat", nargs=2, type=int, default=None,
                        metavar=("MIN", "MAX"),
                        help="Latitude range (integer degrees, e.g. 32 48)")
    parser.add_argument("--lon", nargs=2, type=int, default=None,
                        metavar=("MIN", "MAX"),
                        help="Longitude range (integer degrees, e.g. -118 -110)")
    parser.add_argument("--bands", nargs="+", type=float, default=None,
                        help="Altitude bands to analyze (MSL feet)")
    parser.add_argument("--agl", type=float, default=terrain_intel.DEFAULT_AGL_FT,
                        help="AGL clearance in feet (default: 1000)")
    parser.add_argument("--check", nargs=4, type=float, default=None,
                        metavar=("FROM_LAT", "FROM_LON", "TO_LAT", "TO_LON"),
                        help="Quick viability check (no precomputation)")
    parser.add_argument("--msl", type=float, default=5000,
                        help="MSL for viability check (default: 5000)")
    parser.add_argument("--summary", action="store_true",
                        help="Show coverage summary and exit")
    args = parser.parse_args()

    terrain_intel.init_db()

    if args.summary:
        s = terrain_intel.coverage_summary()
        print("Terrain Intelligence Coverage:")
        print(f"  Sectors analyzed:      {s['sectors']}")
        print(f"  Connectivity records:  {s['connectivity_records']}")
        print(f"  Viable connections:    {s['connectivity_viable']}")
        print(f"  Altitude bands:        {s['altitude_bands']}")
        if s["lat_range"]:
            print(f"  Latitude range:        {s['lat_range'][0]}° to {s['lat_range'][1]}°")
            print(f"  Longitude range:       {s['lon_range'][0]}° to {s['lon_range'][1]}°")
        return

    if args.check:
        from_lat, from_lon, to_lat, to_lon = args.check
        print(f"Checking viability: ({from_lat:.2f}, {from_lon:.2f}) → ({to_lat:.2f}, {to_lon:.2f}) at {args.msl:.0f}' MSL")
        result = terrain_intel.check_viability(from_lat, from_lon, to_lat, to_lon, args.msl, args.agl)
        print(f"  Viable:          {result['viable']}")
        print(f"  Analyzed:        {result['analyzed']}")
        print(f"  Min viable MSL:  {result['min_viable_msl']}")
        print(f"  Explanation:     {result['explanation']}")
        if result.get("sector_path"):
            path_str = " → ".join(f"({s[0]},{s[1]})" for s in result["sector_path"])
            print(f"  Sector path:     {path_str}")
        return

    # ── Precomputation ──
    if args.lat:
        lat_range = (min(args.lat), max(args.lat))
    else:
        # Default: western US
        lat_range = (31, 49)

    if args.lon:
        lon_range = (min(args.lon), max(args.lon))
    else:
        lon_range = (-125, -104)

    bands = args.bands or terrain_intel.ALTITUDE_BANDS

    n_sectors = (lat_range[1] - lat_range[0] + 1) * (lon_range[1] - lon_range[0] + 1)
    print(f"Precomputing terrain intelligence")
    print(f"  Region:    {lat_range[0]}°N–{lat_range[1]}°N, {abs(lon_range[1])}°W–{abs(lon_range[0])}°W")
    print(f"  Sectors:   {n_sectors}")
    print(f"  Bands:     {bands}")
    print(f"  AGL:       {args.agl:.0f} ft")
    print()

    t0 = time.time()
    result = terrain_intel.precompute_region(
        lat_range=lat_range,
        lon_range=lon_range,
        altitude_bands=bands,
        agl_ft=args.agl,
        progress_cb=_progress,
    )
    elapsed = time.time() - t0

    print(f"\n\nDone in {elapsed:.1f}s")
    print(f"  Sectors analyzed:       {result['sectors_analyzed']} (of {result['sectors_total']})")
    print(f"  Connectivity computed:  {result['connectivity_computed']}")
    print(f"  Connectivity skipped:   {result['connectivity_skipped']} (already cached)")
    print(f"  Connectivity total:     {result['connectivity_total']}")

    # Show a quick viability summary for common cross-mountain routes
    print("\n── Quick viability spot-checks ──")
    test_routes = [
        ("KSZT corridor (ID→AZ)", 48.18, -116.56, 32.80, -111.58),
        ("Montana→Idaho (Missoula→Boise)", 46.92, -114.08, 43.56, -116.22),
        ("Oregon→Nevada (Bend→Reno)", 44.10, -121.20, 39.50, -119.77),
        ("SoCal→Phoenix", 33.94, -118.41, 33.43, -112.02),
    ]
    for label, flat, flon, tlat, tlon in test_routes:
        for msl in [5000, 7000, 9000, 12500]:
            v = terrain_intel.check_viability(flat, flon, tlat, tlon, msl, args.agl)
            status = "✓" if v["viable"] else "✗"
            extra = ""
            if not v["viable"] and v["min_viable_msl"]:
                extra = f" (min: {v['min_viable_msl']:.0f}')"
            if not v["analyzed"]:
                status = "?"
            print(f"  {status} {label} @ {msl}'{extra}")


if __name__ == "__main__":
    main()
