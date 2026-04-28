"""
Helicopter Performance Database
================================
Models real-world helicopter performance from publicly available AFM data.
Accounts for density altitude (temperature + pressure altitude) to determine:
  - Whether the aircraft can hover OGE/IGE
  - Achievable rate of climb
  - Adjusted cruise speed
  - Adjusted fuel burn
  - Service ceiling
  - Max gross weight at altitude/temperature

Density Altitude Methodology
-----------------------------
ISA standard temperature at sea level = 15 °C, lapse rate = 1.98 °C / 1000 ft.
ISA temp at altitude = 15 - (1.98 × altitude_ft / 1000)
DA = PA + (120 × (OAT_C − ISA_temp_C))

Performance Interpolation
--------------------------
Each helicopter stores performance data at key density-altitude breakpoints.
Between breakpoints we linearly interpolate. Above the highest breakpoint the
aircraft is at or beyond its ceiling.
"""

from __future__ import annotations
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Density-altitude helpers
# ---------------------------------------------------------------------------

def isa_temp_c(pressure_alt_ft: float) -> float:
    """ISA standard temperature at a given pressure altitude."""
    return 15.0 - 1.98 * (pressure_alt_ft / 1000.0)


def density_altitude_ft(pressure_alt_ft: float, oat_c: float) -> float:
    """Density altitude from pressure altitude and outside air temp (°C)."""
    isa = isa_temp_c(pressure_alt_ft)
    return pressure_alt_ft + 120.0 * (oat_c - isa)


def isa_deviation_c(pressure_alt_ft: float, oat_c: float) -> float:
    """How many °C above/below ISA."""
    return oat_c - isa_temp_c(pressure_alt_ft)


# ---------------------------------------------------------------------------
# Performance-at-altitude data point
# ---------------------------------------------------------------------------

@dataclass
class PerfPoint:
    """Performance snapshot at a given density altitude."""
    da_ft: float            # density altitude (ft)
    max_roc_fpm: float      # max rate of climb (ft/min)
    cruise_ktas: float      # cruise true airspeed (kt)
    fuel_burn_gph: float    # cruise fuel burn (gal/hr)
    hoge_max_gw_lb: float   # max gross weight for HOGE
    hige_max_gw_lb: float   # max gross weight for HIGE


# ---------------------------------------------------------------------------
# Helicopter model
# ---------------------------------------------------------------------------

@dataclass
class HelicopterModel:
    """A helicopter type with performance data from publicly available AFMs."""
    type_code: str               # e.g. "S300C", "R22", "R44", "R66", "B206"
    name: str                    # e.g. "Schweizer S-300C (Hughes 269C)"
    engine_type: str             # "piston" or "turbine"
    fuel_type: str               # "100LL" or "JETA"
    max_gross_weight_lb: float   # structural max gross weight
    empty_weight_lb: float       # typical empty weight (equipped)
    usable_fuel_gal: float       # max usable fuel
    fuel_weight_lb_per_gal: float  # 6.0 for avgas, 6.7 for Jet-A
    service_ceiling_da_ft: float # density-altitude service ceiling
    autorotation_glide_ratio: float = 4.0  # horizontal:vertical glide ratio in autorotation
    autorotation_speed_kt: float = 60.0      # best autorotation glide speed (KIAS)
    airframe_class: str = "helicopter"  # "helicopter" or "fixed_wing" — disables HOGE/HIGE checks for FW
    # Optional chart-derived envelope: [(gross_weight_lb, ceiling_da_ft), ...]
    # If present, this is preferred over proxy estimation.
    enroute_ceiling_curve: List[Tuple[float, float]] = field(default_factory=list)
    enroute_ceiling_curve_source: str = ""
    # Performance breakpoints (sorted ascending by da_ft)
    perf_table: List[PerfPoint] = field(default_factory=list)
    notes: str = ""              # source / caveats

    # ----- derived helpers -----

    def max_fuel_weight_lb(self) -> float:
        return self.usable_fuel_gal * self.fuel_weight_lb_per_gal

    def max_payload_lb(self, fuel_gal: float | None = None) -> float:
        """Max payload given fuel load.  None = full fuel."""
        fg = fuel_gal if fuel_gal is not None else self.usable_fuel_gal
        return self.max_gross_weight_lb - self.empty_weight_lb - fg * self.fuel_weight_lb_per_gal

    # ----- interpolation -----

    def _interp(self, da: float, field_name: str) -> Optional[float]:
        """Linear interpolation of a PerfPoint field by density altitude."""
        if not self.perf_table:
            return None
        pts = self.perf_table
        if da <= pts[0].da_ft:
            return getattr(pts[0], field_name)
        if da >= pts[-1].da_ft:
            top = getattr(pts[-1], field_name)
            # Above last breakpoint: ROC degrades to 0 at service ceiling
            if field_name == "max_roc_fpm" and da <= self.service_ceiling_da_ft:
                remaining = self.service_ceiling_da_ft - pts[-1].da_ft
                if remaining > 0:
                    frac = (da - pts[-1].da_ft) / remaining
                    return top * max(0.0, 1.0 - frac)
                return 0.0
            if da > self.service_ceiling_da_ft and field_name == "max_roc_fpm":
                return 0.0
            return top
        # Find bracketing pair
        for i in range(len(pts) - 1):
            if pts[i].da_ft <= da <= pts[i + 1].da_ft:
                t = (da - pts[i].da_ft) / (pts[i + 1].da_ft - pts[i].da_ft)
                v0 = getattr(pts[i], field_name)
                v1 = getattr(pts[i + 1], field_name)
                return v0 + t * (v1 - v0)
        return getattr(pts[-1], field_name)

    def _interp_curve_by_weight(self, gross_weight_lb: float) -> Optional[float]:
        """Linear interpolation on optional chart-derived ceiling curve."""
        if not self.enroute_ceiling_curve:
            return None
        pts = sorted(self.enroute_ceiling_curve, key=lambda p: p[0])
        if gross_weight_lb <= pts[0][0]:
            return pts[0][1]
        if gross_weight_lb >= pts[-1][0]:
            return pts[-1][1]
        for i in range(len(pts) - 1):
            w0, c0 = pts[i]
            w1, c1 = pts[i + 1]
            if w0 <= gross_weight_lb <= w1 and w1 > w0:
                t = (gross_weight_lb - w0) / (w1 - w0)
                return c0 + t * (c1 - c0)
        return pts[-1][1]

    def max_roc_fpm(self, da: float) -> float:
        """Max rate of climb at a given density altitude."""
        v = self._interp(da, "max_roc_fpm")
        return max(0.0, v) if v is not None else 0.0

    def cruise_ktas(self, da: float) -> float:
        v = self._interp(da, "cruise_ktas")
        return v if v is not None else 0.0

    def fuel_burn_gph(self, da: float) -> float:
        v = self._interp(da, "fuel_burn_gph")
        return v if v is not None else 0.0

    def hoge_max_gw(self, da: float) -> float:
        v = self._interp(da, "hoge_max_gw_lb")
        return v if v is not None else 0.0

    def hige_max_gw(self, da: float) -> float:
        v = self._interp(da, "hige_max_gw_lb")
        return v if v is not None else 0.0

    # ----- high-level checks -----

    def can_hover_oge(self, da: float, gross_weight_lb: float) -> bool:
        return gross_weight_lb <= self.hoge_max_gw(da)

    def can_hover_ige(self, da: float, gross_weight_lb: float) -> bool:
        return gross_weight_lb <= self.hige_max_gw(da)

    def can_reach_altitude(self, pressure_alt_ft: float, oat_c: float) -> bool:
        """Can the aircraft reach this pressure altitude at the given OAT?"""
        da = density_altitude_ft(pressure_alt_ft, oat_c)
        return da < self.service_ceiling_da_ft

    def estimated_enroute_ceiling_da_ft(self, gross_weight_lb: float | None = None) -> float:
        """
        Estimate weight-adjusted enroute ceiling in density altitude.

        This uses the published service ceiling as the max-gross baseline and
        scales additional high-altitude margin from the HIGE degradation visible
        in the highest tabulated performance point. It is intentionally a
        conservative planning proxy, not a serial-number-specific AFM result.
        """
        gw = gross_weight_lb or self.max_gross_weight_lb
        gw = max(1.0, min(gw, self.max_gross_weight_lb))

        chart_ceiling_da = self._interp_curve_by_weight(gw)
        if chart_ceiling_da is not None:
            return max(0.0, chart_ceiling_da)

        if not self.perf_table:
            return self.service_ceiling_da_ft

        top = self.perf_table[-1]
        top_gap_da = max(0.0, self.service_ceiling_da_ft - top.da_ft)
        if top_gap_da <= 0:
            return self.service_ceiling_da_ft

        ref_relief = (self.max_gross_weight_lb - top.hige_max_gw_lb) / self.max_gross_weight_lb
        if ref_relief <= 0:
            return self.service_ceiling_da_ft

        weight_relief = (self.max_gross_weight_lb - gw) / self.max_gross_weight_lb
        boost_da = (weight_relief / ref_relief) * top_gap_da if weight_relief > 0 else 0.0
        boost_da = min(boost_da, self.service_ceiling_da_ft * 0.20)
        return self.service_ceiling_da_ft + max(0.0, boost_da)

    def estimated_enroute_ceiling_msl_ft(
        self,
        oat_c: float,
        gross_weight_lb: float | None = None,
    ) -> float:
        """Convert estimated enroute ceiling DA to an estimated pressure altitude."""
        ceiling_da = self.estimated_enroute_ceiling_da_ft(gross_weight_lb)
        denom = 1.0 + (120.0 * 1.98 / 1000.0)
        return (ceiling_da - 120.0 * (oat_c - 15.0)) / denom

    def performance_at(
        self,
        pressure_alt_ft: float,
        oat_c: float,
        gross_weight_lb: float | None = None,
    ) -> Dict:
        """Return a snapshot of performance at given conditions."""
        da = density_altitude_ft(pressure_alt_ft, oat_c)
        isa_dev = isa_deviation_c(pressure_alt_ft, oat_c)
        gw = gross_weight_lb or self.max_gross_weight_lb
        roc = self.max_roc_fpm(da)
        cruise = self.cruise_ktas(da)
        burn = self.fuel_burn_gph(da)
        hoge_gw = self.hoge_max_gw(da)
        hige_gw = self.hige_max_gw(da)
        est_ceiling_da = self.estimated_enroute_ceiling_da_ft(gw)
        est_ceiling_msl = self.estimated_enroute_ceiling_msl_ft(oat_c, gw)
        ceiling_basis = (
            "Chart-derived gross-weight to ceiling DA curve"
            if self.enroute_ceiling_curve
            else "Published service ceiling baseline plus HIGE-envelope-derived weight adjustment."
        )

        gw_note = f" (max GW for {self.type_code})" if gross_weight_lb is None else ""

        warnings: List[str] = []
        if da >= self.service_ceiling_da_ft:
            warnings.append(
                f"Density altitude {da:.0f} ft exceeds service ceiling "
                f"({self.service_ceiling_da_ft:.0f} ft DA)."
            )
        if roc < 100 and da < self.service_ceiling_da_ft:
            warnings.append(
                f"Rate of climb only {roc:.0f} fpm at DA {da:.0f} ft — "
                f"marginal performance."
            )
        if gw > hige_gw:
            if self.airframe_class == "fixed_wing":
                pass  # HOGE/HIGE not applicable to fixed-wing aircraft
            else:
                hige_deficit = gw - hige_gw
                warnings.append(
                    f"At {gw:.0f} lb{gw_note}, DA {da:.0f} ft exceeds HIGE limit "
                    f"by {hige_deficit:.0f} lb (max HIGE: {hige_gw:.0f} lb).  "
                    f"Plan for a run-on landing and takeoff.  "
                    f"Reduce {hige_deficit / self.fuel_weight_lb_per_gal:.1f} gal fuel "
                    f"or {hige_deficit:.0f} lb payload to regain hover capability."
                )
        elif gw > hoge_gw:
            if self.airframe_class == "fixed_wing":
                pass
            else:
                hoge_deficit = gw - hoge_gw
                warnings.append(
                    f"At {gw:.0f} lb{gw_note}, cannot hover OGE at DA {da:.0f} ft — "
                    f"exceeds HOGE limit by {hoge_deficit:.0f} lb (max HOGE: {hoge_gw:.0f} lb).  "
                    f"Stay in ground effect or reduce "
                    f"{hoge_deficit / self.fuel_weight_lb_per_gal:.1f} gal fuel / "
                    f"{hoge_deficit:.0f} lb payload.  "
                    f"Caution: avoid OGE maneuvers, pinnacle ops, and confined areas "
                    f"without run-on capability."
                )

        return {
            "type_code": self.type_code,
            "name": self.name,
            "pressure_alt_ft": pressure_alt_ft,
            "oat_c": oat_c,
            "density_alt_ft": round(da),
            "isa_deviation_c": round(isa_dev, 1),
            "max_roc_fpm": round(roc),
            "cruise_ktas": round(cruise, 1),
            "fuel_burn_gph": round(burn, 1),
            "hoge_max_gw_lb": round(hoge_gw),
            "hige_max_gw_lb": round(hige_gw),
            "gross_weight_lb": round(gw),
            "can_hover_oge": gw <= hoge_gw,
            "can_hover_ige": gw <= hige_gw,
            "at_service_ceiling": da >= self.service_ceiling_da_ft,
            "estimated_enroute_ceiling_da_ft": round(est_ceiling_da),
            "estimated_enroute_ceiling_msl_ft": round(est_ceiling_msl),
            "estimated_enroute_ceiling_basis": ceiling_basis,
            "warnings": warnings,
        }

    def to_dict(self) -> Dict:
        """Serialisable summary (for /helicopters API)."""
        return {
            "type_code": self.type_code,
            "name": self.name,
            "engine_type": self.engine_type,
            "fuel_type": self.fuel_type,
            "airframe_class": self.airframe_class,
            "max_gross_weight_lb": self.max_gross_weight_lb,
            "empty_weight_lb": self.empty_weight_lb,
            "usable_fuel_gal": self.usable_fuel_gal,
            "fuel_weight_lb_per_gal": self.fuel_weight_lb_per_gal,
            "service_ceiling_da_ft": self.service_ceiling_da_ft,
            "autorotation_glide_ratio": self.autorotation_glide_ratio,
            "autorotation_speed_kt": self.autorotation_speed_kt,
            "has_chart_enroute_ceiling_curve": len(self.enroute_ceiling_curve) > 0,
            "enroute_ceiling_curve_source": self.enroute_ceiling_curve_source,
            "ceiling_estimate_top_da_ft": self.perf_table[-1].da_ft if self.perf_table else self.service_ceiling_da_ft,
            "ceiling_estimate_top_hige_gw_lb": self.perf_table[-1].hige_max_gw_lb if self.perf_table else self.max_gross_weight_lb,
            "ceiling_estimate_basis": (
                "Chart-derived gross-weight to ceiling DA curve"
                if self.enroute_ceiling_curve
                else "Published service ceiling baseline plus HIGE-envelope-derived weight adjustment."
            ),
            "max_payload_full_fuel_lb": round(self.max_payload_lb(), 1),
            "notes": self.notes,
            "defaults": {
                "cruise_speed_kt": self.perf_table[0].cruise_ktas if self.perf_table else 0,
                "fuel_burn_gph": self.perf_table[0].fuel_burn_gph if self.perf_table else 0,
                "usable_fuel_gal": self.usable_fuel_gal,
            },
        }


# ---------------------------------------------------------------------------
# HELICOPTER DATABASE
# ---------------------------------------------------------------------------
# Sources: RFMs/AFMs that are publicly posted by manufacturers, Pilot
# Operating Handbooks, TCDS, and type-certificate data.  Performance
# numbers are ISA approximations for planning only — always consult
# the actual AFM for your serial-number aircraft.
# ---------------------------------------------------------------------------

_DB: Dict[str, HelicopterModel] = {}
_CEILING_CURVES_PATH = Path(__file__).with_name("helicopter_ceiling_curves.json")


def _register(h: HelicopterModel) -> None:
    _DB[h.type_code] = h


# ── Schweizer S-300C / Hughes 269C ────────────────────────────────────────
_register(HelicopterModel(
    type_code="S300C",
    name="Schweizer S-300C (Hughes 269C)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2050,
    empty_weight_lb=1100,
    usable_fuel_gal=48,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=10600,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=900, cruise_ktas=80, fuel_burn_gph=13.0, hoge_max_gw_lb=2050, hige_max_gw_lb=2050),
        PerfPoint(da_ft=2000, max_roc_fpm=750, cruise_ktas=78, fuel_burn_gph=12.8, hoge_max_gw_lb=1980, hige_max_gw_lb=2050),
        PerfPoint(da_ft=4000, max_roc_fpm=580, cruise_ktas=76, fuel_burn_gph=12.5, hoge_max_gw_lb=1870, hige_max_gw_lb=1970),
        PerfPoint(da_ft=6000, max_roc_fpm=400, cruise_ktas=74, fuel_burn_gph=12.2, hoge_max_gw_lb=1750, hige_max_gw_lb=1860),
        PerfPoint(da_ft=8000, max_roc_fpm=210, cruise_ktas=71, fuel_burn_gph=11.8, hoge_max_gw_lb=1610, hige_max_gw_lb=1740),
        PerfPoint(da_ft=10000,max_roc_fpm=60,  cruise_ktas=68, fuel_burn_gph=11.5, hoge_max_gw_lb=1460, hige_max_gw_lb=1600),
    ],
    notes="S-300C RFM / Hughes 269C POH. Lycoming HIO-360-D1A, 190 HP.",
))


def _load_external_ceiling_curves() -> None:
    """
    Load optional chart-digitized enroute ceiling curves from JSON.

    Expected file: mvp_backend/helicopter_ceiling_curves.json
    Format:
    {
      "S300C": {
        "source": "AFM Figure ...",
        "points": [
          {"gross_weight_lb": 2050, "ceiling_da_ft": 10600},
          {"gross_weight_lb": 1700, "ceiling_da_ft": 11100}
        ]
      }
    }
    """
    if not _CEILING_CURVES_PATH.exists():
        return
    try:
        raw = json.loads(_CEILING_CURVES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(raw, dict):
        return

    for type_code, entry in raw.items():
        heli = _DB.get(str(type_code).upper())
        if heli is None or not isinstance(entry, dict):
            continue
        points = entry.get("points")
        if not isinstance(points, list):
            continue
        curve: List[Tuple[float, float]] = []
        for p in points:
            if not isinstance(p, dict):
                continue
            try:
                w = float(p.get("gross_weight_lb"))
                c = float(p.get("ceiling_da_ft"))
            except (TypeError, ValueError):
                continue
            if w > 0 and c > 0:
                curve.append((w, c))
        if len(curve) >= 2:
            heli.enroute_ceiling_curve = sorted(curve, key=lambda x: x[0])
            src = entry.get("source")
            if isinstance(src, str):
                heli.enroute_ceiling_curve_source = src

# ── Schweizer S-300CBi ────────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="S300CBi",
    name="Schweizer S-300CBi",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2150,
    empty_weight_lb=1150,
    usable_fuel_gal=48,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=11200,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=950, cruise_ktas=82, fuel_burn_gph=13.5, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=2000, max_roc_fpm=800, cruise_ktas=80, fuel_burn_gph=13.2, hoge_max_gw_lb=2080, hige_max_gw_lb=2150),
        PerfPoint(da_ft=4000, max_roc_fpm=640, cruise_ktas=78, fuel_burn_gph=12.8, hoge_max_gw_lb=1960, hige_max_gw_lb=2060),
        PerfPoint(da_ft=6000, max_roc_fpm=460, cruise_ktas=76, fuel_burn_gph=12.4, hoge_max_gw_lb=1830, hige_max_gw_lb=1940),
        PerfPoint(da_ft=8000, max_roc_fpm=270, cruise_ktas=73, fuel_burn_gph=12.0, hoge_max_gw_lb=1690, hige_max_gw_lb=1810),
        PerfPoint(da_ft=10000,max_roc_fpm=100, cruise_ktas=70, fuel_burn_gph=11.6, hoge_max_gw_lb=1540, hige_max_gw_lb=1670),
    ],
    notes="S-300CBi RFM. Lycoming HIO-360-G1A, 190 HP fuel-injected.",
))

# ── Robinson R22 Beta II ──────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="R22",
    name="Robinson R22 Beta II",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=1370,
    empty_weight_lb=890,
    usable_fuel_gal=19.2,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14000,
    autorotation_glide_ratio=4.0,
    autorotation_speed_kt=65.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1000, cruise_ktas=96, fuel_burn_gph=8.0, hoge_max_gw_lb=1370, hige_max_gw_lb=1370),
        PerfPoint(da_ft=2000, max_roc_fpm=850,  cruise_ktas=94, fuel_burn_gph=7.8, hoge_max_gw_lb=1330, hige_max_gw_lb=1370),
        PerfPoint(da_ft=4000, max_roc_fpm=700,  cruise_ktas=92, fuel_burn_gph=7.5, hoge_max_gw_lb=1270, hige_max_gw_lb=1340),
        PerfPoint(da_ft=6000, max_roc_fpm=530,  cruise_ktas=89, fuel_burn_gph=7.2, hoge_max_gw_lb=1200, hige_max_gw_lb=1290),
        PerfPoint(da_ft=8000, max_roc_fpm=350,  cruise_ktas=86, fuel_burn_gph=6.9, hoge_max_gw_lb=1120, hige_max_gw_lb=1220),
        PerfPoint(da_ft=10000,max_roc_fpm=180,  cruise_ktas=82, fuel_burn_gph=6.6, hoge_max_gw_lb=1030, hige_max_gw_lb=1140),
        PerfPoint(da_ft=12000,max_roc_fpm=50,   cruise_ktas=78, fuel_burn_gph=6.3, hoge_max_gw_lb=930,  hige_max_gw_lb=1050),
    ],
    notes="R22 Beta II POH. Lycoming O-360-J2A, 131 HP derated.",
))

# ── Robinson R44 Raven II ─────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="R44",
    name="Robinson R44 Raven II",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2500,
    empty_weight_lb=1530,
    usable_fuel_gal=48.5,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14000,
    autorotation_speed_kt=70.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1000, cruise_ktas=110, fuel_burn_gph=15.0, hoge_max_gw_lb=2500, hige_max_gw_lb=2500),
        PerfPoint(da_ft=2000, max_roc_fpm=850,  cruise_ktas=108, fuel_burn_gph=14.7, hoge_max_gw_lb=2420, hige_max_gw_lb=2500),
        PerfPoint(da_ft=4000, max_roc_fpm=700,  cruise_ktas=106, fuel_burn_gph=14.3, hoge_max_gw_lb=2310, hige_max_gw_lb=2430),
        PerfPoint(da_ft=6000, max_roc_fpm=550,  cruise_ktas=103, fuel_burn_gph=13.9, hoge_max_gw_lb=2180, hige_max_gw_lb=2330),
        PerfPoint(da_ft=8000, max_roc_fpm=380,  cruise_ktas=100, fuel_burn_gph=13.4, hoge_max_gw_lb=2030, hige_max_gw_lb=2210),
        PerfPoint(da_ft=10000,max_roc_fpm=200,  cruise_ktas=96,  fuel_burn_gph=12.9, hoge_max_gw_lb=1870, hige_max_gw_lb=2070),
        PerfPoint(da_ft=12000,max_roc_fpm=70,   cruise_ktas=91,  fuel_burn_gph=12.4, hoge_max_gw_lb=1700, hige_max_gw_lb=1910),
    ],
    notes="R44 Raven II POH. Lycoming IO-540-AE1A5, 245 HP fuel-injected.",
))

# ── Robinson R66 Turbine ─────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="R66",
    name="Robinson R66 Turbine",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=2700,
    empty_weight_lb=1280,
    usable_fuel_gal=73.4,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=14000,
    autorotation_glide_ratio=4.5,
    autorotation_speed_kt=72.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1200, cruise_ktas=110, fuel_burn_gph=22.0, hoge_max_gw_lb=2700, hige_max_gw_lb=2700),
        PerfPoint(da_ft=2000, max_roc_fpm=1050, cruise_ktas=108, fuel_burn_gph=21.5, hoge_max_gw_lb=2620, hige_max_gw_lb=2700),
        PerfPoint(da_ft=4000, max_roc_fpm=900,  cruise_ktas=106, fuel_burn_gph=21.0, hoge_max_gw_lb=2510, hige_max_gw_lb=2640),
        PerfPoint(da_ft=6000, max_roc_fpm=730,  cruise_ktas=104, fuel_burn_gph=20.5, hoge_max_gw_lb=2380, hige_max_gw_lb=2540),
        PerfPoint(da_ft=8000, max_roc_fpm=550,  cruise_ktas=101, fuel_burn_gph=20.0, hoge_max_gw_lb=2230, hige_max_gw_lb=2420),
        PerfPoint(da_ft=10000,max_roc_fpm=360,  cruise_ktas=97,  fuel_burn_gph=19.4, hoge_max_gw_lb=2060, hige_max_gw_lb=2280),
        PerfPoint(da_ft=12000,max_roc_fpm=170,  cruise_ktas=93,  fuel_burn_gph=18.8, hoge_max_gw_lb=1880, hige_max_gw_lb=2120),
    ],
    notes="R66 POH. RR300 turboshaft, 270 SHP (derated).",
))

# ── Bell 206B3 JetRanger III ─────────────────────────────────────────────
_register(HelicopterModel(
    type_code="B206",
    name="Bell 206B3 JetRanger III",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=3200,
    empty_weight_lb=1900,
    usable_fuel_gal=76,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=13500,
    autorotation_glide_ratio=4.5,
    autorotation_speed_kt=72.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1350, cruise_ktas=115, fuel_burn_gph=27.0, hoge_max_gw_lb=3200, hige_max_gw_lb=3200),
        PerfPoint(da_ft=2000, max_roc_fpm=1170, cruise_ktas=113, fuel_burn_gph=26.5, hoge_max_gw_lb=3100, hige_max_gw_lb=3200),
        PerfPoint(da_ft=4000, max_roc_fpm=990,  cruise_ktas=111, fuel_burn_gph=25.8, hoge_max_gw_lb=2950, hige_max_gw_lb=3120),
        PerfPoint(da_ft=6000, max_roc_fpm=800,  cruise_ktas=108, fuel_burn_gph=25.1, hoge_max_gw_lb=2780, hige_max_gw_lb=2990),
        PerfPoint(da_ft=8000, max_roc_fpm=600,  cruise_ktas=105, fuel_burn_gph=24.3, hoge_max_gw_lb=2590, hige_max_gw_lb=2840),
        PerfPoint(da_ft=10000,max_roc_fpm=390,  cruise_ktas=101, fuel_burn_gph=23.5, hoge_max_gw_lb=2370, hige_max_gw_lb=2660),
        PerfPoint(da_ft=12000,max_roc_fpm=180,  cruise_ktas=96,  fuel_burn_gph=22.7, hoge_max_gw_lb=2130, hige_max_gw_lb=2450),
    ],
    notes="Bell 206B3 RFM. Allison 250-C20J, 420 SHP (derated to 317 SHP).",
))

# ── Bell 407 ──────────────────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="B407",
    name="Bell 407",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=5250,
    empty_weight_lb=2707,
    usable_fuel_gal=141.5,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=18500,
    autorotation_glide_ratio=5.0,
    autorotation_speed_kt=80.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1800, cruise_ktas=133, fuel_burn_gph=44.0, hoge_max_gw_lb=5250, hige_max_gw_lb=5250),
        PerfPoint(da_ft=2000, max_roc_fpm=1600, cruise_ktas=131, fuel_burn_gph=43.0, hoge_max_gw_lb=5100, hige_max_gw_lb=5250),
        PerfPoint(da_ft=4000, max_roc_fpm=1380, cruise_ktas=129, fuel_burn_gph=42.0, hoge_max_gw_lb=4900, hige_max_gw_lb=5150),
        PerfPoint(da_ft=6000, max_roc_fpm=1150, cruise_ktas=126, fuel_burn_gph=41.0, hoge_max_gw_lb=4680, hige_max_gw_lb=4980),
        PerfPoint(da_ft=8000, max_roc_fpm=910,  cruise_ktas=123, fuel_burn_gph=39.8, hoge_max_gw_lb=4430, hige_max_gw_lb=4780),
        PerfPoint(da_ft=10000,max_roc_fpm=660,  cruise_ktas=120, fuel_burn_gph=38.5, hoge_max_gw_lb=4150, hige_max_gw_lb=4550),
        PerfPoint(da_ft=12000,max_roc_fpm=410,  cruise_ktas=116, fuel_burn_gph=37.0, hoge_max_gw_lb=3850, hige_max_gw_lb=4290),
        PerfPoint(da_ft=14000,max_roc_fpm=180,  cruise_ktas=111, fuel_burn_gph=35.4, hoge_max_gw_lb=3520, hige_max_gw_lb=3990),
    ],
    notes="Bell 407 RFM. RR250-C47B, 813 SHP (derated).",
))

# ── Airbus AS350 B3e / H125 ──────────────────────────────────────────────
_register(HelicopterModel(
    type_code="H125",
    name="Airbus H125 (AS350 B3e)",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=5512,
    empty_weight_lb=2876,
    usable_fuel_gal=143,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=23000,
    autorotation_glide_ratio=5.0,
    autorotation_speed_kt=80.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1800, cruise_ktas=130, fuel_burn_gph=48.0, hoge_max_gw_lb=5512, hige_max_gw_lb=5512),
        PerfPoint(da_ft=4000, max_roc_fpm=1450, cruise_ktas=127, fuel_burn_gph=46.0, hoge_max_gw_lb=5200, hige_max_gw_lb=5400),
        PerfPoint(da_ft=8000, max_roc_fpm=1050, cruise_ktas=123, fuel_burn_gph=43.0, hoge_max_gw_lb=4750, hige_max_gw_lb=5100),
        PerfPoint(da_ft=12000,max_roc_fpm=650,  cruise_ktas=118, fuel_burn_gph=40.0, hoge_max_gw_lb=4200, hige_max_gw_lb=4700),
        PerfPoint(da_ft=16000,max_roc_fpm=300,  cruise_ktas=112, fuel_burn_gph=37.0, hoge_max_gw_lb=3600, hige_max_gw_lb=4200),
        PerfPoint(da_ft=20000,max_roc_fpm=80,   cruise_ktas=104, fuel_burn_gph=34.0, hoge_max_gw_lb=2900, hige_max_gw_lb=3600),
    ],
    notes="H125 / AS350 B3e AFM. Arriel 2D, 847 SHP. Known for high-altitude work.",
))

# ── MD 500E (Hughes 369E) ────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="MD500E",
    name="MD 500E (Hughes 369E)",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=3000,
    empty_weight_lb=1590,
    usable_fuel_gal=62,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=16000,
    autorotation_glide_ratio=4.5,
    autorotation_speed_kt=80.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1700, cruise_ktas=135, fuel_burn_gph=28.0, hoge_max_gw_lb=3000, hige_max_gw_lb=3000),
        PerfPoint(da_ft=2000, max_roc_fpm=1480, cruise_ktas=133, fuel_burn_gph=27.3, hoge_max_gw_lb=2900, hige_max_gw_lb=3000),
        PerfPoint(da_ft=4000, max_roc_fpm=1250, cruise_ktas=131, fuel_burn_gph=26.6, hoge_max_gw_lb=2770, hige_max_gw_lb=2920),
        PerfPoint(da_ft=6000, max_roc_fpm=1010, cruise_ktas=128, fuel_burn_gph=25.8, hoge_max_gw_lb=2620, hige_max_gw_lb=2810),
        PerfPoint(da_ft=8000, max_roc_fpm=770,  cruise_ktas=124, fuel_burn_gph=25.0, hoge_max_gw_lb=2450, hige_max_gw_lb=2680),
        PerfPoint(da_ft=10000,max_roc_fpm=520,  cruise_ktas=120, fuel_burn_gph=24.1, hoge_max_gw_lb=2260, hige_max_gw_lb=2530),
        PerfPoint(da_ft=12000,max_roc_fpm=270,  cruise_ktas=115, fuel_burn_gph=23.2, hoge_max_gw_lb=2050, hige_max_gw_lb=2360),
        PerfPoint(da_ft=14000,max_roc_fpm=80,   cruise_ktas=109, fuel_burn_gph=22.2, hoge_max_gw_lb=1820, hige_max_gw_lb=2170),
    ],
    notes="MD 500E RFM. Allison 250-C20B, 420 SHP.",
))

# ── Enstrom 280FX Shark ──────────────────────────────────────────────────
_register(HelicopterModel(
    type_code="F28F",
    name="Enstrom 280FX Shark",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2350,
    empty_weight_lb=1515,
    usable_fuel_gal=42,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=12000,
    autorotation_speed_kt=75.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=950,  cruise_ktas=100, fuel_burn_gph=14.0, hoge_max_gw_lb=2350, hige_max_gw_lb=2350),
        PerfPoint(da_ft=2000, max_roc_fpm=790,  cruise_ktas=98,  fuel_burn_gph=13.7, hoge_max_gw_lb=2260, hige_max_gw_lb=2350),
        PerfPoint(da_ft=4000, max_roc_fpm=630,  cruise_ktas=96,  fuel_burn_gph=13.3, hoge_max_gw_lb=2140, hige_max_gw_lb=2280),
        PerfPoint(da_ft=6000, max_roc_fpm=460,  cruise_ktas=93,  fuel_burn_gph=12.9, hoge_max_gw_lb=2000, hige_max_gw_lb=2170),
        PerfPoint(da_ft=8000, max_roc_fpm=290,  cruise_ktas=89,  fuel_burn_gph=12.4, hoge_max_gw_lb=1840, hige_max_gw_lb=2040),
        PerfPoint(da_ft=10000,max_roc_fpm=120,  cruise_ktas=85,  fuel_burn_gph=11.9, hoge_max_gw_lb=1670, hige_max_gw_lb=1890),
    ],
    notes="Enstrom 280FX POH. Lycoming HIO-360-F1AD, 225 HP.",
))

# ── Bell UH-1H Iroquois (Huey) ──────────────────────────────────────────
_register(HelicopterModel(
    type_code="UH1H",
    name="Bell UH-1H Iroquois (Huey)",
    engine_type="turbine",
    fuel_type="JETA",
    max_gross_weight_lb=9500,
    empty_weight_lb=5210,
    usable_fuel_gal=209,
    fuel_weight_lb_per_gal=6.7,
    service_ceiling_da_ft=12600,
    autorotation_glide_ratio=4.0,
    autorotation_speed_kt=90.0,
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=1660, cruise_ktas=110, fuel_burn_gph=80.0, hoge_max_gw_lb=8500, hige_max_gw_lb=9500),
        PerfPoint(da_ft=2000, max_roc_fpm=1420, cruise_ktas=108, fuel_burn_gph=78.0, hoge_max_gw_lb=8100, hige_max_gw_lb=9200),
        PerfPoint(da_ft=4000, max_roc_fpm=1180, cruise_ktas=106, fuel_burn_gph=76.0, hoge_max_gw_lb=7650, hige_max_gw_lb=8800),
        PerfPoint(da_ft=6000, max_roc_fpm=930,  cruise_ktas=103, fuel_burn_gph=74.0, hoge_max_gw_lb=7150, hige_max_gw_lb=8300),
        PerfPoint(da_ft=8000, max_roc_fpm=670,  cruise_ktas=100, fuel_burn_gph=71.0, hoge_max_gw_lb=6600, hige_max_gw_lb=7750),
        PerfPoint(da_ft=10000,max_roc_fpm=400,  cruise_ktas=96,  fuel_burn_gph=68.0, hoge_max_gw_lb=6000, hige_max_gw_lb=7100),
        PerfPoint(da_ft=12000,max_roc_fpm=130,  cruise_ktas=91,  fuel_burn_gph=65.0, hoge_max_gw_lb=5350, hige_max_gw_lb=6400),
    ],
    notes="UH-1H Operator's Manual TM 55-1520-210-10. Lycoming T53-L-13B, 1,400 SHP.",
))


# ===========================================================================
# FIXED-WING TRAINERS / LIGHT SINGLES
# ===========================================================================
# These low-powered light singles struggle at high density altitude almost as
# much as light helicopters. HOGE/HIGE columns are set equal to max gross
# weight (hover checks are not applicable to fixed wing) and suppressed by
# `airframe_class="fixed_wing"` in performance_at(). Service ceilings are
# the published POH service ceiling (lightly loaded). Climb rates degrade
# realistically with DA based on POH performance tables (best-rate climb at
# Vy at the gross weight shown, ISA, fixed-pitch prop, leaned for max power).
# ===========================================================================

# \u2500\u2500 Cessna 152 \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="C152",
    name="Cessna 152 (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=1670,
    empty_weight_lb=1115,
    usable_fuel_gal=24.5,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14700,
    autorotation_glide_ratio=9.0,   # power-off glide ratio (best glide ~60 KIAS)
    autorotation_speed_kt=60.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=715, cruise_ktas=107, fuel_burn_gph=6.1, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=2000, max_roc_fpm=605, cruise_ktas=106, fuel_burn_gph=5.9, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=4000, max_roc_fpm=500, cruise_ktas=105, fuel_burn_gph=5.7, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=6000, max_roc_fpm=395, cruise_ktas=103, fuel_burn_gph=5.5, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=8000, max_roc_fpm=290, cruise_ktas=101, fuel_burn_gph=5.3, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=10000,max_roc_fpm=190, cruise_ktas=98,  fuel_burn_gph=5.1, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
        PerfPoint(da_ft=12000,max_roc_fpm=90,  cruise_ktas=94,  fuel_burn_gph=4.9, hoge_max_gw_lb=1670, hige_max_gw_lb=1670),
    ],
    notes="Cessna 152 POH (1985). Lycoming O-235-L2C, 110 HP. Climb rates at gross. Mountain warning: at DA above ~10,000 ft this airplane has very little excess power for terrain avoidance.",
))

# \u2500\u2500 Cessna 150 \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="C150",
    name="Cessna 150M (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=1600,
    empty_weight_lb=1111,
    usable_fuel_gal=22.5,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14000,
    autorotation_glide_ratio=8.5,
    autorotation_speed_kt=60.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=670, cruise_ktas=102, fuel_burn_gph=6.0, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=2000, max_roc_fpm=560, cruise_ktas=101, fuel_burn_gph=5.8, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=4000, max_roc_fpm=450, cruise_ktas=100, fuel_burn_gph=5.6, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=6000, max_roc_fpm=345, cruise_ktas=98,  fuel_burn_gph=5.4, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=8000, max_roc_fpm=240, cruise_ktas=95,  fuel_burn_gph=5.2, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=10000,max_roc_fpm=140, cruise_ktas=92,  fuel_burn_gph=5.0, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
        PerfPoint(da_ft=12000,max_roc_fpm=50,  cruise_ktas=88,  fuel_burn_gph=4.8, hoge_max_gw_lb=1600, hige_max_gw_lb=1600),
    ],
    notes="Cessna 150M POH. Continental O-200-A, 100 HP. Even more anemic than the 152 at altitude \u2014 not a mountain airplane.",
))

# \u2500\u2500 Cessna 172N (160 HP) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="C172N",
    name="Cessna 172N Skyhawk 160 HP (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2300,
    empty_weight_lb=1430,
    usable_fuel_gal=40,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14200,
    autorotation_glide_ratio=9.0,
    autorotation_speed_kt=65.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=770, cruise_ktas=120, fuel_burn_gph=8.4, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=2000, max_roc_fpm=655, cruise_ktas=119, fuel_burn_gph=8.1, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=4000, max_roc_fpm=540, cruise_ktas=118, fuel_burn_gph=7.8, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=6000, max_roc_fpm=425, cruise_ktas=116, fuel_burn_gph=7.5, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=8000, max_roc_fpm=310, cruise_ktas=113, fuel_burn_gph=7.2, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=10000,max_roc_fpm=200, cruise_ktas=110, fuel_burn_gph=6.9, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
        PerfPoint(da_ft=12000,max_roc_fpm=90,  cruise_ktas=106, fuel_burn_gph=6.6, hoge_max_gw_lb=2300, hige_max_gw_lb=2300),
    ],
    notes="Cessna 172N POH. Lycoming O-320-H2AD, 160 HP. The classic underpowered trainer at altitude \u2014 plan generous climb gradients.",
))

# \u2500\u2500 Cessna 172SP (180 HP) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="C172SP",
    name="Cessna 172SP Skyhawk 180 HP (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2550,
    empty_weight_lb=1660,
    usable_fuel_gal=53,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14000,
    autorotation_glide_ratio=9.0,
    autorotation_speed_kt=68.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=730, cruise_ktas=124, fuel_burn_gph=10.0, hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=2000, max_roc_fpm=620, cruise_ktas=123, fuel_burn_gph=9.7,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=4000, max_roc_fpm=510, cruise_ktas=121, fuel_burn_gph=9.4,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=6000, max_roc_fpm=400, cruise_ktas=119, fuel_burn_gph=9.1,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=8000, max_roc_fpm=290, cruise_ktas=117, fuel_burn_gph=8.8,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=10000,max_roc_fpm=180, cruise_ktas=113, fuel_burn_gph=8.5,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=12000,max_roc_fpm=70,  cruise_ktas=109, fuel_burn_gph=8.2,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
    ],
    notes="Cessna 172S POH. Lycoming IO-360-L2A, 180 HP. Better than 172N at altitude but still draggy at gross.",
))

# \u2500\u2500 Cessna 182T (Skylane, 230 HP) \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="C182T",
    name="Cessna 182T Skylane (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=3100,
    empty_weight_lb=2000,
    usable_fuel_gal=87,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=18100,
    autorotation_glide_ratio=9.0,
    autorotation_speed_kt=70.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=924, cruise_ktas=145, fuel_burn_gph=14.0, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=2000, max_roc_fpm=815, cruise_ktas=144, fuel_burn_gph=13.6, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=4000, max_roc_fpm=705, cruise_ktas=143, fuel_burn_gph=13.2, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=6000, max_roc_fpm=595, cruise_ktas=141, fuel_burn_gph=12.8, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=8000, max_roc_fpm=485, cruise_ktas=139, fuel_burn_gph=12.4, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=10000,max_roc_fpm=375, cruise_ktas=136, fuel_burn_gph=12.0, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=12000,max_roc_fpm=265, cruise_ktas=133, fuel_burn_gph=11.6, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=14000,max_roc_fpm=155, cruise_ktas=129, fuel_burn_gph=11.2, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
        PerfPoint(da_ft=16000,max_roc_fpm=50,  cruise_ktas=124, fuel_burn_gph=10.8, hoge_max_gw_lb=3100, hige_max_gw_lb=3100),
    ],
    notes="Cessna 182T POH. Lycoming IO-540-AB1A5, 230 HP. Real mountain capability when lightly loaded.",
))

# \u2500\u2500 Piper PA-28-140 Cherokee Cruiser \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="PA28140",
    name="Piper PA-28-140 Cherokee 150 HP (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2150,
    empty_weight_lb=1275,
    usable_fuel_gal=48,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14300,
    autorotation_glide_ratio=8.5,
    autorotation_speed_kt=70.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=660, cruise_ktas=115, fuel_burn_gph=8.0, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=2000, max_roc_fpm=550, cruise_ktas=114, fuel_burn_gph=7.8, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=4000, max_roc_fpm=440, cruise_ktas=113, fuel_burn_gph=7.5, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=6000, max_roc_fpm=335, cruise_ktas=111, fuel_burn_gph=7.3, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=8000, max_roc_fpm=230, cruise_ktas=108, fuel_burn_gph=7.0, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=10000,max_roc_fpm=130, cruise_ktas=105, fuel_burn_gph=6.8, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
        PerfPoint(da_ft=12000,max_roc_fpm=40,  cruise_ktas=101, fuel_burn_gph=6.5, hoge_max_gw_lb=2150, hige_max_gw_lb=2150),
    ],
    notes="Piper PA-28-140 POH. Lycoming O-320-E2A, 150 HP. Cherokee 140s with 4 seats are routinely flown over gross \u2014 numbers here are at book gross.",
))

# \u2500\u2500 Piper PA-28-161 Warrior III \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="PA28161",
    name="Piper PA-28-161 Warrior III (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2440,
    empty_weight_lb=1518,
    usable_fuel_gal=48,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=14000,
    autorotation_glide_ratio=9.0,
    autorotation_speed_kt=73.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=644, cruise_ktas=117, fuel_burn_gph=8.7, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=2000, max_roc_fpm=540, cruise_ktas=116, fuel_burn_gph=8.4, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=4000, max_roc_fpm=430, cruise_ktas=115, fuel_burn_gph=8.1, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=6000, max_roc_fpm=320, cruise_ktas=113, fuel_burn_gph=7.8, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=8000, max_roc_fpm=215, cruise_ktas=110, fuel_burn_gph=7.5, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=10000,max_roc_fpm=110, cruise_ktas=107, fuel_burn_gph=7.2, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
        PerfPoint(da_ft=12000,max_roc_fpm=20,  cruise_ktas=104, fuel_burn_gph=6.9, hoge_max_gw_lb=2440, hige_max_gw_lb=2440),
    ],
    notes="Piper Warrior III POH. Lycoming O-320-D3G, 160 HP. Climb performance at gross weight.",
))

# \u2500\u2500 Piper PA-28-181 Archer III \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
_register(HelicopterModel(
    type_code="PA28181",
    name="Piper PA-28-181 Archer III (fixed-wing)",
    engine_type="piston",
    fuel_type="100LL",
    max_gross_weight_lb=2550,
    empty_weight_lb=1635,
    usable_fuel_gal=48,
    fuel_weight_lb_per_gal=6.0,
    service_ceiling_da_ft=13650,
    autorotation_glide_ratio=9.0,
    autorotation_speed_kt=76.0,
    airframe_class="fixed_wing",
    perf_table=[
        PerfPoint(da_ft=0,    max_roc_fpm=667, cruise_ktas=126, fuel_burn_gph=10.5, hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=2000, max_roc_fpm=560, cruise_ktas=125, fuel_burn_gph=10.2, hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=4000, max_roc_fpm=450, cruise_ktas=123, fuel_burn_gph=9.9,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=6000, max_roc_fpm=340, cruise_ktas=121, fuel_burn_gph=9.6,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=8000, max_roc_fpm=230, cruise_ktas=119, fuel_burn_gph=9.3,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=10000,max_roc_fpm=125, cruise_ktas=116, fuel_burn_gph=9.0,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
        PerfPoint(da_ft=12000,max_roc_fpm=30,  cruise_ktas=112, fuel_burn_gph=8.7,  hoge_max_gw_lb=2550, hige_max_gw_lb=2550),
    ],
    notes="Piper Archer III POH. Lycoming O-360-A4M, 180 HP.",
))

_load_external_ceiling_curves()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_helicopter(type_code: str) -> Optional[HelicopterModel]:
    """Look up a helicopter by type code (case-insensitive)."""
    return _DB.get(type_code.upper())


def list_helicopters() -> List[Dict]:
    """Return summary dicts for all helicopters in the database."""
    return [h.to_dict() for h in sorted(_DB.values(), key=lambda h: h.name)]


def evaluate_leg(
    type_code: str,
    dep_elev_ft: float,
    arr_elev_ft: float,
    max_enroute_elev_ft: float,
    oat_c: float,
    gross_weight_lb: float | None = None,
    fuel_burn_gal: float = 0.0,
) -> Dict:
    """
    Evaluate whether a helicopter can fly a leg given the conditions.

    Uses takeoff weight for departure/enroute and landing weight
    (takeoff weight minus fuel burned) for arrival.

    Returns a dict with per-phase performance and any warnings/blockers.
    """
    heli = get_helicopter(type_code)
    if heli is None:
        return {"error": f"Unknown helicopter type: {type_code}"}

    gw = gross_weight_lb or heli.max_gross_weight_lb
    ldg_wt = gw - fuel_burn_gal * heli.fuel_weight_lb_per_gal
    results: Dict = {
        "type_code": heli.type_code,
        "gross_weight_lb": round(gw),
        "landing_weight_lb": round(ldg_wt),
        "oat_c": oat_c,
        "phases": {},
        "warnings": [],
        "blockers": [],
    }

    # Evaluate three phases: departure at takeoff weight,
    # enroute at takeoff weight, arrival at landing weight
    phases = [
        ("departure", dep_elev_ft, gw),
        ("enroute_max", max_enroute_elev_ft, gw),
        ("arrival", arr_elev_ft, ldg_wt),
    ]
    for phase_name, pa, phase_gw in phases:
        perf = heli.performance_at(pa, oat_c, phase_gw)
        results["phases"][phase_name] = perf
        for w in perf["warnings"]:
            tagged = f"[{phase_name}] {w}"
            if perf.get("at_service_ceiling") or "cannot hover" in w.lower():
                results["blockers"].append(tagged)
            else:
                results["warnings"].append(tagged)

    results["flyable"] = len(results["blockers"]) == 0
    return results
