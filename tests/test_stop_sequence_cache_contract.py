from pathlib import Path

PLANNER = Path(__file__).resolve().parents[1] / "mvp_backend" / "planner.py"


def main() -> None:
    src = PLANNER.read_text(encoding="utf-8")
    fn_start = src.index("def plan_stop_sequences(")
    fn_end = src.index("def plan_stop_sequence(", fn_start)
    body = src[fn_start:fn_end]
    assert "route_cache.is_known_failure" not in body
    assert "return (from_code, to_code) in _blocked" in body
    print("stop sequence cache contract ok")


if __name__ == "__main__":
    main()
