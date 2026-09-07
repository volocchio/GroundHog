from mvp_backend.planner import _outside_conus_for_vfr_planning


def main() -> None:
    # North shore / Ontario side of Lake Erie should be blocked when avoiding borders.
    assert _outside_conus_for_vfr_planning(42.45, -81.50) is True
    # South shore / Ohio side should remain allowed.
    assert _outside_conus_for_vfr_planning(41.60, -81.50) is False
    # Basic CONUS bounds still apply.
    assert _outside_conus_for_vfr_planning(50.0, -100.0) is True
    assert _outside_conus_for_vfr_planning(40.0, -100.0) is False


if __name__ == "__main__":
    main()
    print("border mask ok")
