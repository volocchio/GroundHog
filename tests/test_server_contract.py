from fastapi import HTTPException

from mvp_backend.server import RouteRequest, app, route


def _base_req(**overrides):
    data = {
        "dep_icao": "N47",
        "arr_icao": "KSZT",
        "cruise_speed_kt": 75,
        "usable_fuel_gal": 48,
        "fuel_burn_gph": 13,
        "reserve_min": 30,
        "min_agl_ft": 200,
        "max_msl_ft": 6000,
        "required_fuel": "100LL",
        # Explicitly disable streaming-only defaults so legacy route can be
        # exercised only for its supported subset.
        "avoid_airspace": [],
        "avoid_borders": False,
        "avoid_tfrs": False,
        "obstacle_radius_nm": 0,
    }
    data.update(overrides)
    return RouteRequest(**data)


def test_health_route_defined_once():
    health_routes = [r for r in app.routes if getattr(r, "path", None) == "/health"]
    assert len(health_routes) == 1


def test_legacy_route_rejects_streaming_only_constraints():
    req = _base_req(waypoints=["KCOE"], avoid_borders=True)
    try:
        route(req)
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "/route/stream" in str(exc.detail)
        assert "waypoints" in str(exc.detail)
        assert "avoid_borders" in str(exc.detail)
    else:
        raise AssertionError("legacy /route accepted streaming-only constraints")


def main() -> None:
    test_health_route_defined_once()
    test_legacy_route_rejects_streaming_only_constraints()
    print("server contract ok")


if __name__ == "__main__":
    main()
