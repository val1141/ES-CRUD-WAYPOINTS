import uuid
import sys
import os
from fastapi.testclient import TestClient
from clickhouse_connect.driver.exceptions import Error as ClickHouseError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import app.main as main

app = main.app

# Override DB dependency with dummy object

def override_get_db():
    class Dummy:
        def close(self):
            pass
    yield Dummy()

app.dependency_overrides[main.get_db] = override_get_db
client = TestClient(app)


def test_get_route_points_returns_404(monkeypatch):
    def fake_get_active_points_for_route(client, route_id):
        return []

    monkeypatch.setattr(main, "get_active_points_for_route", fake_get_active_points_for_route)
    route_id = uuid.uuid4()
    response = client.get(f"/routes/{route_id}/points")
    assert response.status_code == 404
    assert "No points" in response.json()["detail"]


def test_get_point_history_returns_404(monkeypatch):
    def fake_get_point_event_history(client, route_id, point_id):
        return []

    monkeypatch.setattr(main, "get_point_event_history", fake_get_point_event_history)
    route_id = uuid.uuid4()
    point_id = uuid.uuid4()
    response = client.get(f"/routes/{route_id}/points/{point_id}/history")
    assert response.status_code == 404
    assert "No history" in response.json()["detail"]


def test_clickhouse_error_translates_to_404(monkeypatch):
    def fake_error(client, route_id):
        raise ClickHouseError("Table doesn't exist")

    monkeypatch.setattr(main, "get_active_points_for_route", fake_error)
    route_id = uuid.uuid4()
    response = client.get(f"/routes/{route_id}/points")
    assert response.status_code == 404
    assert response.json()["detail"] == "Requested resource not found"


def test_clickhouse_error_translates_to_500(monkeypatch):
    def fake_error(client, route_id):
        raise ClickHouseError("Some other error")

    monkeypatch.setattr(main, "get_active_points_for_route", fake_error)
    route_id = uuid.uuid4()
    response = client.get(f"/routes/{route_id}/points")
    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"
