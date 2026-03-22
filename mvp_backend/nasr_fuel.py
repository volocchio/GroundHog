"""NASR (FAA 28-day subscription) APT.txt parser for fuel types.

We extract:
- FAA Location Identifier (LID) at offset 28, len 4 (e.g. E60, S75, SZT)
- ICAO identifier (field at offset 1211, len 7) - empty for many US airports
- Public-use indicator (field name "AIRPORT USE" in layout; we locate by offset once confirmed)
- Fuel types (A70, offset 901 len 40)

MVP: we only care about 100LL vs Jet-A family.

NOTE: Offsets in apt_rf.txt are 1-indexed. We convert to 0-index slices.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


# Based on extract/Layout_Data/apt_rf.txt in NASR subscription
OFF_FUEL_A70 = 901 - 1
LEN_FUEL_A70 = 40

OFF_ICAO = 1211 - 1
LEN_ICAO = 7

# FAA Location Identifier (LID) - present on ALL airports (e.g. E60, S75, SZT)
OFF_LID = 28 - 1
LEN_LID = 4

# Facility use (A18) at offset 186, len 2. Values: PU (public) / PR (private)
OFF_FACILITY_USE_A18 = 186 - 1
LEN_FACILITY_USE_A18 = 2


@dataclass(frozen=True)
class FuelInfo:
    lid: str            # FAA Location Identifier (always present)
    icao: str           # ICAO code (may be empty for non-ICAO airports)
    facility_use: str   # PU/PR/etc
    fuel_100ll: bool
    fuel_jeta: bool
    raw_fuel: str


def _parse_fuel_tokens(raw: str) -> set[str]:
    # NASR fuel field: up to 8 fixed-width 5-char codes (e.g. "100LLA    MOGAS...")
    # Always chunk by 5 chars since tokens run together without separators
    toks = [raw[i:i+5].strip().replace("_", "") for i in range(0, len(raw), 5)]
    return {t for t in toks if t}


def decode_fuel_info_from_apt_record(line: str) -> FuelInfo | None:
    if not line.startswith("APT"):
        return None
    lid = line[OFF_LID:OFF_LID + LEN_LID].strip().upper()
    icao = line[OFF_ICAO:OFF_ICAO + LEN_ICAO].strip().upper()
    # Need at least one identifier
    if not lid and not icao:
        return None

    facility_use = line[OFF_FACILITY_USE_A18:OFF_FACILITY_USE_A18 + LEN_FACILITY_USE_A18].strip().upper()

    raw_fuel = line[OFF_FUEL_A70:OFF_FUEL_A70 + LEN_FUEL_A70]
    toks = _parse_fuel_tokens(raw_fuel)

    fuel_100ll = "100LL" in toks

    # Jet-A family: A, A+, A++, A++10, A1, A1+
    fuel_jeta = any(t.startswith("A") for t in toks)

    return FuelInfo(
        lid=lid,
        icao=icao,
        facility_use=facility_use,
        fuel_100ll=fuel_100ll,
        fuel_jeta=fuel_jeta,
        raw_fuel=" ".join(sorted(toks)),
    )


def iter_fuel_info(apt_txt_path: str) -> Iterable[FuelInfo]:
    with open(apt_txt_path, "r", encoding="latin-1", errors="ignore") as f:
        for line in f:
            if not line.startswith("APT"):
                continue
            info = decode_fuel_info_from_apt_record(line)
            if info:
                yield info
