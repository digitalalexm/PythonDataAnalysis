from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from api.main import app, get_repository
from api.repository import MetricsRepository
from pipeline.models import MetricRow


def _row(service, minute, request_count=1, avg_latency_ms=100.0, error_rate=0.0):
    return MetricRow(
        service=service,
        minute=datetime(2025, 1, 12, 10, minute, tzinfo=timezone.utc),
        request_count=request_count,
        avg_latency_ms=avg_latency_ms,
        error_rate=error_rate,
    )


FIXTURE_ROWS = [
    _row("checkout", 0),
    _row("checkout", 1),
    _row("payments", 0, avg_latency_ms=None, error_rate=None),
]


@pytest.fixture
def client():
    app.dependency_overrides[get_repository] = lambda: MetricsRepository(FIXTURE_ROWS)
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_metrics_with_no_filters_returns_everything(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert len(response.json()) == 3


def test_metrics_filters_by_service(client):
    response = client.get("/metrics", params={"service": "payments"})
    body = response.json()
    assert len(body) == 1
    assert body[0]["service"] == "payments"
    assert body[0]["avg_latency_ms"] is None


def test_metrics_filters_by_time_range(client):
    response = client.get(
        "/metrics",
        params={"from": "2025-01-12T10:01:00Z", "to": "2025-01-12T10:01:00Z"},
    )
    body = response.json()
    assert len(body) == 1
    assert body[0]["service"] == "checkout"
    assert body[0]["minute"] == "2025-01-12T10:01:00Z"


def test_metrics_with_unmatched_service_returns_empty_list_not_404(client):
    response = client.get("/metrics", params={"service": "does-not-exist"})
    assert response.status_code == 200
    assert response.json() == []


def test_metrics_rejects_from_after_to(client):
    response = client.get(
        "/metrics",
        params={"from": "2025-01-12T10:05:00Z", "to": "2025-01-12T10:00:00Z"},
    )
    assert response.status_code == 400
