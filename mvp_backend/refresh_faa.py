#!/usr/bin/env python3
"""Download latest FAA NASR data (28-day cycle) and rebuild airports + obstacles.

Run via cron:  0 6 1,15 * * cd /root/.../GroundHog && python3 -m mvp_backend.refresh_faa

Data sources:
  - NASR (APT.txt)  → airports_solver.csv   (28-day cycle)
  - DOF (DOF.CSV)   → obstacles.sqlite      (56-day cycle)
  - Airspace ArcGIS → airspace.sqlite        (28-day cycle)
"""
import io
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# ── FAA NASR download ─────────────────────────────────────────────────

NASR_DISTRIBUTION_URL = "https://soa.smext.faa.gov/apra/nfdc/nasr/chart?edition=current"
FAA_DIR = os.path.join(ROOT, "faa_nasr")
EXTRACT_DIR = os.path.join(FAA_DIR, "extract")


def download_nasr():
    """Download current NASR subscription ZIP and extract APT.txt."""
    print("Downloading FAA NASR data...", flush=True)
    os.makedirs(EXTRACT_DIR, exist_ok=True)

    # The FAA NASR API returns a JSON with the download URL
    headers = {"Accept": "application/json"}
    r = requests.get(NASR_DISTRIBUTION_URL, headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json()

    # Get the download URL from the response
    download_url = data.get("edition", [{}])[0].get("product", {}).get("url")
    if not download_url:
        # Fallback: direct 28-day subscription ZIP
        download_url = "https://nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_" + _current_cycle() + ".zip"

    print(f"  URL: {download_url}", flush=True)
    r2 = requests.get(download_url, timeout=300, stream=True)
    r2.raise_for_status()

    content = r2.content
    print(f"  Downloaded {len(content) / 1024 / 1024:.1f} MB", flush=True)

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        # Extract APT.txt
        for name in z.namelist():
            if name.upper().endswith("APT.TXT"):
                z.extract(name, EXTRACT_DIR)
                # Move to expected location
                src = os.path.join(EXTRACT_DIR, name)
                dst = os.path.join(EXTRACT_DIR, "APT.txt")
                if src != dst:
                    shutil.move(src, dst)
                print(f"  Extracted APT.txt ({os.path.getsize(dst) / 1024 / 1024:.1f} MB)", flush=True)
                break
        else:
            print("  WARNING: APT.txt not found in NASR ZIP!", file=sys.stderr, flush=True)
            return False
    return True


def _current_cycle():
    """Approximate current 28-day cycle date (YYYY-MM-DD)."""
    import datetime
    # FAA cycles start Jan 30, 2020 and repeat every 28 days
    epoch = datetime.date(2020, 1, 30)
    today = datetime.date.today()
    days = (today - epoch).days
    cycle_num = days // 28
    cycle_start = epoch + datetime.timedelta(days=cycle_num * 28)
    return cycle_start.strftime("%Y-%m-%d")


# ── DOF (Digital Obstacle File) ───────────────────────────────────────

DOF_URL = "https://aeronav.faa.gov/Obst_Data/DAILY_DOF_CSV.ZIP"
OBS_DIR = os.path.join(ROOT, "mvp_backend", "obstacle_data")


def download_dof():
    """Download latest DOF and rebuild obstacles.sqlite."""
    print("Downloading FAA DOF data...", flush=True)
    os.makedirs(OBS_DIR, exist_ok=True)

    r = requests.get(DOF_URL, timeout=300)
    r.raise_for_status()
    print(f"  Downloaded {len(r.content) / 1024 / 1024:.1f} MB", flush=True)

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for name in z.namelist():
            if name.upper().endswith(".CSV"):
                z.extract(name, OBS_DIR)
                src = os.path.join(OBS_DIR, name)
                dst = os.path.join(OBS_DIR, "DOF.CSV")
                if src != dst:
                    shutil.move(src, dst)
                print(f"  Extracted DOF.CSV ({os.path.getsize(dst) / 1024 / 1024:.1f} MB)", flush=True)
                break

    # Rebuild obstacles.sqlite from DOF.CSV
    _build_obstacles_db()
    return True


def _build_obstacles_db():
    """Parse DOF.CSV → obstacles.sqlite."""
    import csv
    import sqlite3

    csv_path = os.path.join(OBS_DIR, "DOF.CSV")
    db_path = os.path.join(OBS_DIR, "obstacles.sqlite")

    if not os.path.exists(csv_path):
        print("  WARNING: DOF.CSV not found, skipping obstacle DB build", file=sys.stderr)
        return

    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE obstacles (
            id INTEGER PRIMARY KEY,
            lat REAL, lon REAL,
            agl INTEGER, amsl INTEGER,
            type TEXT, lit INTEGER
        )
    """)

    count = 0
    with open(csv_path, "r", encoding="latin-1", errors="ignore") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            try:
                lat = float(row.get("LATITUDE", "") or 0)
                lon = float(row.get("LONGITUDE", "") or 0)
                agl = int(float(row.get("AGL_HT", "") or 0))
                amsl = int(float(row.get("AMSL_HT", "") or 0))
                otype = (row.get("TYPE_CODE", "") or "").strip()
                lit = 1 if (row.get("LIGHTING", "") or "").strip().upper() in ("R", "Y", "M", "H", "S", "F") else 0
            except (ValueError, TypeError):
                continue
            if lat == 0 or lon == 0:
                continue
            batch.append((lat, lon, agl, amsl, otype, lit))
            if len(batch) >= 10000:
                conn.executemany("INSERT INTO obstacles (lat,lon,agl,amsl,type,lit) VALUES (?,?,?,?,?,?)", batch)
                count += len(batch)
                batch = []
        if batch:
            conn.executemany("INSERT INTO obstacles (lat,lon,agl,amsl,type,lit) VALUES (?,?,?,?,?,?)", batch)
            count += len(batch)

    conn.execute("CREATE INDEX idx_obs_bbox ON obstacles (lat, lon)")
    conn.execute("CREATE INDEX idx_obs_agl ON obstacles (agl)")
    conn.commit()
    conn.close()
    print(f"  Built obstacles.sqlite: {count} obstacles, "
          f"{os.path.getsize(db_path) / 1024 / 1024:.1f} MB", flush=True)


# ── Airspace refresh ─────────────────────────────────────────────────

def refresh_airspace():
    """Re-download airspace from FAA ArcGIS."""
    print("Refreshing airspace data...", flush=True)
    script = os.path.join(ROOT, "mvp_backend", "airspace_data", "download_airspace.py")
    if os.path.exists(script):
        subprocess.run([sys.executable, script], check=True)
        print("  Airspace refreshed.", flush=True)
    else:
        print(f"  WARNING: {script} not found", file=sys.stderr, flush=True)


# ── Rebuild airports_solver.csv ──────────────────────────────────────

def rebuild_airports():
    """Rebuild airports_solver.csv from fresh NASR data."""
    print("Rebuilding airports_solver.csv...", flush=True)
    script = os.path.join(ROOT, "mvp_backend", "build_airports_solver.py")
    if os.path.exists(script):
        subprocess.run([sys.executable, "-m", "mvp_backend.build_airports_solver"],
                       cwd=ROOT, check=True)
        csv_path = os.path.join(ROOT, "mvp_backend", "airports_solver.csv")
        print(f"  airports_solver.csv: {os.path.getsize(csv_path) / 1024 / 1024:.1f} MB", flush=True)
    else:
        print(f"  WARNING: {script} not found", file=sys.stderr, flush=True)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    t0 = time.time()
    print(f"=== FAA Data Refresh  {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} ===\n", flush=True)

    ok = True
    try:
        if download_nasr():
            rebuild_airports()
        else:
            ok = False
    except Exception as e:
        print(f"NASR download failed: {e}", file=sys.stderr, flush=True)
        ok = False

    try:
        download_dof()
    except Exception as e:
        print(f"DOF download failed: {e}", file=sys.stderr, flush=True)
        ok = False

    try:
        refresh_airspace()
    except Exception as e:
        print(f"Airspace refresh failed: {e}", file=sys.stderr, flush=True)
        ok = False

    elapsed = time.time() - t0
    status = "SUCCESS" if ok else "PARTIAL FAILURE"
    print(f"\n=== {status}  ({elapsed:.0f}s) ===", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
