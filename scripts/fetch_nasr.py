#!/usr/bin/env python3
"""Download the FAA NASR APT.txt file (Legacy TXT format).

Scrapes the FAA 28-Day NASR Subscription page for the current cycle date,
downloads the APT.zip, and extracts APT.txt to faa_nasr/extract/.
"""

import io
import re
import sys
import zipfile
from pathlib import Path

import requests

SUBSCRIPTION_INDEX = (
    "https://www.faa.gov/air_traffic/flight_info/aeronav/"
    "aero_data/NASR_Subscription/"
)
APT_ZIP_TEMPLATE = (
    "https://nfdc.faa.gov/webContent/28DaySub/{date}/APT.zip"
)

REPO_ROOT = Path(__file__).resolve().parent.parent
EXTRACT_DIR = REPO_ROOT / "faa_nasr" / "extract"
APT_TXT = EXTRACT_DIR / "APT.txt"


def current_cycle_date() -> str:
    """Scrape the FAA subscription page for the current effective date."""
    print("Fetching current NASR cycle date …")
    resp = requests.get(SUBSCRIPTION_INDEX, timeout=30)
    resp.raise_for_status()
    # Look for the "Current" section link, e.g. .../NASR_Subscription/2026-03-19
    m = re.search(
        r"Current.*?NASR_Subscription/(\d{4}-\d{2}-\d{2})",
        resp.text,
        re.DOTALL,
    )
    if not m:
        sys.exit("ERROR: Could not find current NASR cycle date on FAA page.")
    return m.group(1)


def download_apt(date: str) -> bytes:
    """Download the APT.zip for the given cycle date."""
    url = APT_ZIP_TEMPLATE.format(date=date)
    print(f"Downloading {url} …")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def extract_apt(zip_bytes: bytes) -> None:
    """Extract APT.txt from the zip into faa_nasr/extract/."""
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        apt_name = next((n for n in names if n.upper().endswith("APT.TXT")), None)
        if apt_name is None:
            sys.exit(f"ERROR: APT.txt not found in zip. Contents: {names}")
        print(f"Extracting {apt_name} → {APT_TXT}")
        APT_TXT.write_bytes(zf.read(apt_name))


def main() -> None:
    if APT_TXT.exists():
        print(f"{APT_TXT} already exists. Delete it first to re-download.")
        return

    date = current_cycle_date()
    print(f"Current cycle: {date}")
    zip_bytes = download_apt(date)
    extract_apt(zip_bytes)
    print("Done ✓")


if __name__ == "__main__":
    main()
