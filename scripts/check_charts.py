#!/usr/bin/env python3
"""
check_charts.py  —  Verify FAA VFR chart tile services are alive and current.

GroundHog uses externally-hosted ArcGIS tile services for VFR charts.
The FAA publishes new VFR Sectional editions on a 56-day cycle.
This script checks that the tile services are responding and that
the tiles reflect the latest FAA edition.

Run on the FAA 56-day schedule (or anytime) to confirm charts are current.

Chart sources:
  VFR Sectional tiles:
    https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer
    Hosted by FAA (orgId ssFJjBXIUyZDrSYZ) on ArcGIS Online.
    Zoom 8-12 only.  CORS enabled.  Auto-updated by FAA every 56 days.

  Navigation Charts (WAC replacement):
    https://services.arcgisonline.com/arcgis/rest/services/Specialty/World_Navigation_Charts/MapServer
    NGA Operational Navigation Charts at 1:1,000,000.  Hosted by ESRI.
    All zoom levels.  Open CORS (*).

  FAA edition schedule / DOLE:
    https://www.faa.gov/air_traffic/flight_info/aeronav/productcatalog/doles/AutoDOLE/
    https://aeronav.faa.gov/visual/          (directory of edition folders by date)

  FAA GeoTIFF raster downloads (not used by GroundHog, but available):
    https://aeronav.faa.gov/visual/{MM-DD-YYYY}/All_Files/
    https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/vfr/
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Tile service endpoints
# ---------------------------------------------------------------------------

VFR_SECTIONAL_SERVICE = (
    "https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ"
    "/arcgis/rest/services/VFR_Sectional/MapServer"
)
VFR_SECTIONAL_SAMPLE_TILE = VFR_SECTIONAL_SERVICE + "/tile/8/97/52"   # Phoenix area

VFR_SERVICES_LIST = (
    "https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ"
    "/arcgis/rest/services?f=json"
)

NAV_CHARTS_SERVICE = (
    "https://services.arcgisonline.com/arcgis/rest/services"
    "/Specialty/World_Navigation_Charts/MapServer"
)
NAV_CHARTS_SAMPLE_TILE = NAV_CHARTS_SERVICE + "/tile/7/48/26"

# FAA edition directory
FAA_VISUAL_INDEX = "https://aeronav.faa.gov/visual/"

# ---------------------------------------------------------------------------
# Known 56-day cycle anchor — Mar 19 2026 is a known effective date.
# Every edition is exactly 56 days apart.
# ---------------------------------------------------------------------------
CYCLE_ANCHOR = datetime(2026, 3, 19, tzinfo=timezone.utc)
CYCLE_DAYS = 56


def current_edition_date(now=None):
    """Return the most recent FAA 56-day cycle effective date."""
    now = now or datetime.now(timezone.utc)
    delta = (now - CYCLE_ANCHOR).days
    cycles_since = delta // CYCLE_DAYS
    if delta < 0:
        cycles_since -= 1
    return CYCLE_ANCHOR + timedelta(days=cycles_since * CYCLE_DAYS)


def next_edition_date(now=None):
    return current_edition_date(now) + timedelta(days=CYCLE_DAYS)


def _get_json(url, timeout=15):
    req = Request(url, headers={"User-Agent": "GroundHog-ChartCheck/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def _head(url, timeout=15):
    """Return (status_code, headers_dict, content_length)."""
    req = Request(url, method="HEAD",
                  headers={"User-Agent": "GroundHog-ChartCheck/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), int(resp.headers.get("Content-Length", 0))
    except HTTPError as e:
        return e.code, {}, 0


def _get_tile(url, timeout=15):
    """Fetch a tile, return (status, content_type, size, last_modified)."""
    req = Request(url, headers={"User-Agent": "GroundHog-ChartCheck/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "")
            lm = resp.headers.get("Last-Modified", "")
            return resp.status, ct, len(data), lm
    except HTTPError as e:
        return e.code, "", 0, ""


def _parse_http_date(s):
    """Parse an HTTP-date header like 'Thu, 19 Mar 2026 22:05:00 GMT'."""
    if not s:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S GMT"):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def scrape_faa_editions():
    """Scrape https://aeronav.faa.gov/visual/ for published edition folders."""
    req = Request(FAA_VISUAL_INDEX,
                  headers={"User-Agent": "GroundHog-ChartCheck/1.0"})
    try:
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode()
    except (URLError, HTTPError):
        return []
    # Folders look like 03-19-2026/
    dates = []
    for m in re.finditer(r'(\d{2}-\d{2}-\d{4})/', html):
        try:
            dt = datetime.strptime(m.group(1), "%m-%d-%Y").replace(tzinfo=timezone.utc)
            dates.append(dt)
        except ValueError:
            pass
    dates.sort()
    return dates


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_vfr_sectional(verbose=False):
    """Check VFR Sectional tile service health and currency."""
    print("=" * 60)
    print("VFR SECTIONAL CHART TILES")
    print("=" * 60)
    ok = True

    # 1. Service metadata
    try:
        svc = _get_json(VFR_SECTIONAL_SERVICE + "?f=json")
        access = svc.get("access", "UNKNOWN")
        print(f"  Service status:  alive")
        print(f"  Access flag:     {access}")
        if access == "SECURE":
            print("    (tiles still serve publicly despite SECURE flag)")
    except Exception as e:
        print(f"  Service status:  UNREACHABLE — {e}")
        ok = False

    # 2. Sample tile fetch
    status, ct, size, lm = _get_tile(VFR_SECTIONAL_SAMPLE_TILE)
    print(f"  Sample tile:     HTTP {status}, {size:,} bytes, type={ct}")
    if status != 200 or size < 1000:
        print("  !! TILE FETCH FAILED — charts may be broken")
        ok = False
    if lm:
        lm_dt = _parse_http_date(lm)
        print(f"  Tile Last-Modified: {lm}")
        if lm_dt:
            expected = current_edition_date()
            delta = abs((lm_dt - expected).days)
            if delta <= 5:
                print(f"  Edition match:   YES (within {delta} days of {expected.strftime('%b %d %Y')})")
            else:
                print(f"  Edition match:   MAYBE (tiles modified {lm_dt.strftime('%b %d %Y')}, "
                      f"expected ~{expected.strftime('%b %d %Y')})")

    # 3. Service list — check cacheKey for changes
    try:
        svcs = _get_json(VFR_SERVICES_LIST)
        for s in svcs.get("services", []):
            if s.get("name") == "VFR_Sectional":
                created_ms = int(s.get("created", 0))
                created_dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
                print(f"  Cache rebuilt:   {created_dt.strftime('%b %d %Y %H:%M UTC')}")
                print(f"  Cache key:       {s.get('cacheKey', 'N/A')}")
                if verbose:
                    print(f"  Service item ID: {s.get('serviceItemId', 'N/A')}")
                break
    except Exception:
        pass

    print()
    return ok


def check_nav_charts(verbose=False):
    """Check Navigation Charts (WAC replacement) tile service."""
    print("=" * 60)
    print("NAVIGATION CHARTS (NGA ONC)")
    print("=" * 60)
    ok = True

    try:
        svc = _get_json(NAV_CHARTS_SERVICE + "?f=json")
        print(f"  Service status:  alive")
        if verbose:
            desc = svc.get("serviceDescription", "")[:100]
            print(f"  Description:     {desc}...")
    except Exception as e:
        print(f"  Service status:  UNREACHABLE — {e}")
        ok = False

    status, ct, size, lm = _get_tile(NAV_CHARTS_SAMPLE_TILE)
    print(f"  Sample tile:     HTTP {status}, {size:,} bytes, type={ct}")
    if status != 200 or size < 1000:
        print("  !! TILE FETCH FAILED")
        ok = False
    if "image" not in ct:
        print(f"  !! Unexpected content type: {ct}")
        ok = False

    print()
    return ok


def check_faa_schedule():
    """Show FAA chart edition schedule."""
    print("=" * 60)
    print("FAA CHART SCHEDULE (56-day cycle)")
    print("=" * 60)
    now = datetime.now(timezone.utc)
    current = current_edition_date(now)
    nxt = next_edition_date(now)
    days_until = (nxt - now).days

    print(f"  Today:           {now.strftime('%b %d %Y')}")
    print(f"  Current edition: {current.strftime('%b %d %Y')}")
    print(f"  Next edition:    {nxt.strftime('%b %d %Y')} ({days_until} days away)")

    # Scrape FAA for published editions
    editions = scrape_faa_editions()
    if editions:
        print(f"  FAA published:   {', '.join(e.strftime('%b %d %Y') for e in editions[-4:])}")
        latest_faa = editions[-1]
        if latest_faa >= nxt:
            print(f"  Next edition files already on FAA server")
        elif latest_faa >= current:
            print(f"  Current edition files present on FAA server")
        else:
            print(f"  !! FAA server may be behind — latest is {latest_faa.strftime('%b %d %Y')}")

    print()
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Check GroundHog chart tile services are alive and current."
    )
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show extra detail")
    parser.add_argument("--json", action="store_true",
                        help="Output machine-readable JSON summary")
    args = parser.parse_args()

    results = {}
    print()

    ok_schedule = check_faa_schedule()
    ok_vfr = check_vfr_sectional(args.verbose)
    ok_nav = check_nav_charts(args.verbose)

    all_ok = ok_vfr and ok_nav
    now = datetime.now(timezone.utc)

    if args.json:
        results = {
            "timestamp": now.isoformat(),
            "current_edition": current_edition_date(now).strftime("%Y-%m-%d"),
            "next_edition": next_edition_date(now).strftime("%Y-%m-%d"),
            "days_until_next": (next_edition_date(now) - now).days,
            "vfr_sectional_ok": ok_vfr,
            "nav_charts_ok": ok_nav,
            "all_ok": all_ok,
        }
        print(json.dumps(results, indent=2))

    if all_ok:
        print("All chart services are healthy.")
    else:
        print("!! ONE OR MORE CHART SERVICES HAVE ISSUES — see above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
