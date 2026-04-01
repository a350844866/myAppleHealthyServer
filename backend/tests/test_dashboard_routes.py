from __future__ import annotations

from backend.routes import dashboard
from backend.tests.conftest import make_test_client


def test_dashboard_home_wraps_payload(monkeypatch):
    client = make_test_client(dashboard.router)
    monkeypatch.setattr(
        dashboard,
        "get_dashboard_home_payload",
        lambda force_refresh=False: {
            "hero": {"title": "Recovered"},
            "score": {"overall": 88.5, "label": "状态稳"},
        },
    )

    response = client.get("/api/dashboard/home")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["score"]["overall"] == 88.5
    assert payload["data"]["hero"]["title"] == "Recovered"
    assert "generated_at" in payload["meta"]


def test_dashboard_ai_reports_list_response(monkeypatch):
    client = make_test_client(dashboard.router)
    monkeypatch.setattr(
        dashboard,
        "list_recent_ai_reports",
        lambda limit: [{"id": 1, "summary": "cached", "model": "demo"}],
    )

    response = client.get("/api/dashboard/ai-reports?limit=1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["summary"] == "cached"
    assert payload["meta"]["total"] == 1
    assert payload["meta"]["limit"] == 1
