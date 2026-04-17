import pytest
from fastapi.testclient import TestClient
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app

client = TestClient(app)


def test_health_endpoint():
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json()["status"] == "ok"


def test_get_workers():
    res = client.get("/workers/get_workers")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_submit_task():
    payload = {
        "data": {"task": "test"},
        "required_cpu": 2,
        "required_ram": 4,
        "priority": 1,
    }
    res = client.post("/tasks/submit", json=payload)
    assert res.status_code == 201
    assert "id" in res.json()


def test_get_pending_tasks():
    res = client.get("/tasks/tasks")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


def test_rl_metrics():
    res = client.get("/learning/metrics/rl")
    assert res.status_code == 200
    assert "total_updates" in res.json()


def test_get_all_tasks():
    res = client.get("/tasks/all_tasks")
    assert res.status_code == 200
    assert isinstance(res.json(), list)
