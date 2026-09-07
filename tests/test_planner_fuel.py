from mvp_backend.planner import leg_fuel_ok


def main() -> None:
    # 75 NM at 75 kt = 1 hr. Burn at 13 gph is 13 gal.
    # 30 min reserve is 6.5 gal. Feasibility needs 19.5 gal, but reported
    # leg fuel must remain actual burn only: 13 gal.
    ok, time_hr, burn_gal = leg_fuel_ok(
        dist_nm=75,
        cruise_speed_kt=75,
        usable_fuel_gal=48,
        burn_gph=13,
        reserve_min=30,
    )
    assert ok is True
    assert abs(time_hr - 1.0) < 1e-9
    assert abs(burn_gal - 13.0) < 1e-9

    # Feasibility should still include reserve.
    ok, _, burn_gal = leg_fuel_ok(75, 75, usable_fuel_gal=19.0, burn_gph=13, reserve_min=30)
    assert ok is False
    assert abs(burn_gal - 13.0) < 1e-9


if __name__ == "__main__":
    main()
    print("planner fuel ok")
