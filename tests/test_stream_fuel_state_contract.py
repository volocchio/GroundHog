from pathlib import Path

SERVER = Path(__file__).resolve().parents[1] / "mvp_backend" / "server.py"


def main() -> None:
    src = SERVER.read_text(encoding="utf-8")
    assert "current_fuel_gal = (req.fuel_load_gal if req.fuel_load_gal > 0" in src
    assert "start_fuel = current_fuel_gal" in src
    assert "current_fuel_gal = max(0.0, current_fuel_gal - leg_fuel_gal)" in src
    assert "last_leg_time = seg_last_leg_dist / req.cruise_speed_kt" not in src
    print("stream fuel-state contract ok")


if __name__ == "__main__":
    main()
