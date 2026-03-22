from __future__ import annotations

import csv
import os
from typing import Dict

from mvp_backend.nasr_fuel import iter_fuel_info


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TAMARACK_DIR = os.path.join(ROOT, "Tamarack_Mission_Analysis")
FAA_DIR = os.path.join(ROOT, "faa_nasr", "extract")

AIRPORTS_FULL = os.path.join(TAMARACK_DIR, "airports_full.csv")
APT_TXT = os.path.join(FAA_DIR, "APT.txt")

OUT = os.path.join(ROOT, "mvp_backend", "airports_solver.csv")


def main():
    # Load fuel info keyed by ICAO
    fuel_by_icao: Dict[str, dict] = {}
    for fi in iter_fuel_info(APT_TXT):
        if not fi.icao:
            continue
        fuel_by_icao[fi.icao] = {
            "facility_use": fi.facility_use,
            "fuel_100ll": int(fi.fuel_100ll),
            "fuel_jeta": int(fi.fuel_jeta),
            "raw_fuel": fi.raw_fuel,
        }

    # Read airports_full and join by icao_code
    with open(AIRPORTS_FULL, "r", newline="", encoding="utf-8", errors="ignore") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames = list(reader.fieldnames or [])

        # Output schema
        out_fields = [
            "icao",
            "name",
            "type",
            "lat",
            "lon",
            "elevation_ft",
            "municipality",
            "iso_country",
            "facility_use",
            "fuel_100ll",
            "fuel_jeta",
        ]

        with open(OUT, "w", newline="", encoding="utf-8") as f_out:
            w = csv.DictWriter(f_out, fieldnames=out_fields)
            w.writeheader()

            n = 0
            n_join = 0
            n_public = 0
            for row in reader:
                n += 1
                icao = (row.get("icao_code") or "").strip().upper()
                if not icao:
                    # fallback: for many US airports, ident is the ICAO (e.g. KSZT)
                    icao = (row.get("ident") or "").strip().upper()
                if not icao:
                    continue

                lat = row.get("latitude_deg")
                lon = row.get("longitude_deg")
                if not lat or not lon:
                    continue

                fuel = fuel_by_icao.get(icao)
                if not fuel:
                    # no fuel data; still output with zeros
                    fuel = {"facility_use": "", "fuel_100ll": 0, "fuel_jeta": 0}
                else:
                    n_join += 1

                facility_use = fuel.get("facility_use", "")
                if facility_use == "PU":
                    n_public += 1

                w.writerow({
                    "icao": icao,
                    "name": (row.get("name") or "").strip(),
                    "type": (row.get("type") or "").strip(),
                    "lat": float(lat),
                    "lon": float(lon),
                    "elevation_ft": int(float(row.get("elevation_ft") or 0)),
                    "municipality": (row.get("municipality") or "").strip(),
                    "iso_country": (row.get("iso_country") or "").strip(),
                    "facility_use": facility_use,
                    "fuel_100ll": int(fuel.get("fuel_100ll", 0)),
                    "fuel_jeta": int(fuel.get("fuel_jeta", 0)),
                })

    print(f"Wrote {OUT}")
    print(f"Joined fuel rows: {n_join}")
    print(f"Public-use rows (joined): {n_public}")


if __name__ == "__main__":
    main()
