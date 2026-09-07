from pathlib import Path

SERVER = Path(__file__).resolve().parents[1] / "mvp_backend" / "server.py"


def main() -> None:
    src = SERVER.read_text(encoding="utf-8")
    gen_start = src.index("    def generate():")
    gen_end = src.index("    return StreamingResponse(generate()", gen_start)
    body = src[gen_start:gen_end]
    assert "eff_climb_fpm = _da_adjusted_climb_fpm(req, heli)" in body
    # The fuel-stop graph and terrain A* must use the same climb budget.
    assert "max_climb_fpm=req.max_climb_fpm" not in body
    assert body.count("max_climb_fpm=eff_climb_fpm") >= 3
    assert "seg_last_leg_dist" not in body
    print("da climb contract ok")


if __name__ == "__main__":
    main()
