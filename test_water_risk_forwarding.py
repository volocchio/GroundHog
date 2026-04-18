import unittest
from unittest.mock import patch

from mvp_backend import planner


class WaterRiskForwardingTests(unittest.TestCase):
    def test_plan_route_multi_stop_passes_water_settings_to_leg_solver(self):
        dep = planner.Airport("KAAA", "Dep", 34.0, -118.0, 500.0, "PU", 1, 1)
        arr = planner.Airport("KBBB", "Arr", 35.0, -117.0, 500.0, "PU", 1, 1)
        airports = {dep.icao: dep, arr.icao: arr}
        captured = {}

        def fake_leg(*args, **kwargs):
            captured.update(kwargs)
            return planner.LegResult(
                dist_nm=90.0,
                path_latlon=[(dep.lat, dep.lon), (arr.lat, arr.lon)],
            )

        with patch("mvp_backend.planner.terrain_avoid_leg", side_effect=fake_leg):
            result = planner.plan_route_multi_stop(
                dep=dep,
                arr=arr,
                airports=airports,
                cruise_speed_kt=120.0,
                usable_fuel_gal=50.0,
                burn_gph=10.0,
                reserve_min=20.0,
                max_msl_ft=4000.0,
                min_agl_ft=500.0,
                required_fuel="100LL",
                max_detour_factor=2.0,
                glide_ratio=4.5,
                water_risk=100.0,
            )

        self.assertNotEqual(result["type"], "no_route")
        self.assertEqual(captured.get("glide_ratio"), 4.5)
        self.assertEqual(captured.get("water_risk"), 100.0)


if __name__ == "__main__":
    unittest.main()