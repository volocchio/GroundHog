from __future__ import annotations

import csv
import os
from typing import Dict

from mvp_backend.nasr_fuel import iter_fuel_info, OFF_ICAO, LEN_ICAO, OFF_LID, LEN_LID


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FAA_DIR = os.path.join(ROOT, "faa_nasr", "extract")
APT_TXT = os.path.join(FAA_DIR, "APT.txt")

OUT = os.path.join(ROOT, "mvp_backend", "airports_solver.csv")

# Offsets from apt_rf.txt (1-indexed -> 0-index)
OFF_NAME_A2 = 134 - 1
LEN_NAME_A2 = 50
OFF_TYPE = 15 - 1
LEN_TYPE = 13
OFF_LAT_SEC = 539 - 1
LEN_LAT_SEC = 12
OFF_LON_SEC = 566 - 1
LEN_LON_SEC = 12
OFF_ELEV_A21 = 579 - 1
LEN_ELEV_A21 = 7


def _sec_to_deg(sec_str: str) -> float:
    s = sec_str.strip().upper()
    if not s:
        return float("nan")
    hemi = s[-1]
    val = float(s[:-1])
    deg = val / 3600.0
    if hemi in ("S", "W"):
        deg = -deg
    return deg


def main():
    # Write directly from FAA APT.txt (authoritative for ICAO + public-use + fuel)
    out_fields = [
        "icao",
        "lid",
        "name",
        "type",
        "lat",
        "lon",
        "elevation_ft",
        "facility_use",
        "fuel_100ll",
        "fuel_jeta",
    ]

    n = n_public = 0

    with open(OUT, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=out_fields)
        w.writeheader()

    # Build fuel lookup keyed by LID (present on all airports)
    fuel_by_lid: Dict[str, dict] = {}
    for fi in iter_fuel_info(APT_TXT):
        key = fi.lid or fi.icao
        if not key:
            continue
        fuel_by_lid[key] = {
            "facility_use": fi.facility_use,
            "fuel_100ll": int(fi.fuel_100ll),
            "fuel_jeta": int(fi.fuel_jeta),
        }

    with open(OUT, "w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=out_fields)
        w.writeheader()

        with open(APT_TXT, "r", encoding="latin-1", errors="ignore") as f:
            for line in f:
                if not line.startswith("APT"):
                    continue
                lid = line[OFF_LID:OFF_LID + LEN_LID].strip().upper()
                icao = line[OFF_ICAO:OFF_ICAO + LEN_ICAO].strip().upper()
                if not lid and not icao:
                    continue

                fuel = fuel_by_lid.get(lid) or fuel_by_lid.get(icao)
                if not fuel:
                    continue

                facility_use = (fuel.get("facility_use") or "").strip().upper()

                name = line[OFF_NAME_A2:OFF_NAME_A2 + LEN_NAME_A2].strip()
                typ = line[OFF_TYPE:OFF_TYPE + LEN_TYPE].strip()

                lat = _sec_to_deg(line[OFF_LAT_SEC:OFF_LAT_SEC + LEN_LAT_SEC])
                lon = _sec_to_deg(line[OFF_LON_SEC:OFF_LON_SEC + LEN_LON_SEC])
                if lat != lat or lon != lon:
                    continue

                elev_raw = line[OFF_ELEV_A21:OFF_ELEV_A21 + LEN_ELEV_A21].strip()
                try:
                    elev_ft = int(round(float(elev_raw))) if elev_raw else 0
                except ValueError:
                    elev_ft = 0

                # Use ICAO if available, otherwise LID
                w.writerow({
                    "icao": icao or lid,
                    "lid": lid,
                    "name": name,
                    "type": typ,
                    "lat": lat,
                    "lon": lon,
                    "elevation_ft": elev_ft,
                    "facility_use": facility_use,
                    "fuel_100ll": int(fuel.get("fuel_100ll", 0)),
                    "fuel_jeta": int(fuel.get("fuel_jeta", 0)),
                })
                n += 1
                if facility_use == "PU":
                    n_public += 1

    print(f"Wrote {OUT}")
    print(f"Total airports written: {n}")
    print(f"Public-use airports: {n_public}")
    print(f"Private airports: {n - n_public}")


if __name__ == "__main__":
    main()
