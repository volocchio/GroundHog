import os
import tempfile

from fastapi.testclient import TestClient
import mvp_backend.server as server


def main() -> None:
    assert "user_data" in server.AIRCRAFT_STORE_PATH
    with tempfile.TemporaryDirectory() as td:
        server.AIRCRAFT_STORE_PATH = os.path.join(td, "saved_aircraft.json")
        c = TestClient(server.app)
        assert c.get("/aircraft").json() == {}
        profile = {"base_type": "S300C", "bew_lb": 1100, "pilot_lb": 200, "cg": {}}
        r = c.put("/aircraft/N123GH", json=profile)
        assert r.status_code == 200, r.text
        assert r.json()["reg"] == "N123GH"
        data = c.get("/aircraft").json()
        assert data["N123GH"]["base_type"] == "S300C"
        r = c.delete("/aircraft/N123GH")
        assert r.status_code == 200, r.text
        assert r.json()["deleted"] is True
        assert c.get("/aircraft").json() == {}
    print("aircraft store ok")


if __name__ == "__main__":
    main()
