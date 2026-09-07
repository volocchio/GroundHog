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
    assert "r.water_risk != null ? r.water_risk : 100" in html
    assert "const terrainFollow = document.querySelector" in html
    assert "max_climb_fpm: maxClimbFpm" in html
    assert "terrain_follow: terrainFollow" in html
    assert 'id="mapPlanProgress"' in html
    assert "const mapBox = document.getElementById('mapPlanProgress')" in html
    assert "if (mapBox) mapBox.classList.add('active')" in html
    assert "#mapPlanProgress.active" in html
    assert "top: 46px" in html
    assert "fetch('/aircraft')" in html
    assert "fetch('/aircraft/' + encodeURIComponent(cleanReg)" in html
    assert "fetch('/aircraft/' + encodeURIComponent(reg), { method: 'DELETE' })" in html
    assert "localStorage.setItem(AIRCRAFT_KEY" not in html


if __name__ == "__main__":
    main()
    print("webapp contract ok")
