from pathlib import Path

WEBAPP = Path(__file__).resolve().parents[1] / "mvp_backend" / "webapp.html"


def main() -> None:
    html = WEBAPP.read_text(encoding="utf-8")
    plan_start = html.index("async function plan()")
    plan_end = html.index("document.getElementById('goBtn').addEventListener", plan_start)
    plan_body = html[plan_start:plan_end]

    assert "await planStreaming(planningPayload);" in plan_body
    assert "await planNormal(payload);" not in plan_body
    assert "document.getElementById('watchSearch').checked" not in plan_body
    assert 'id="maxMsl" type="number" value="6000"' in html


if __name__ == "__main__":
    main()
    print("webapp contract ok")
