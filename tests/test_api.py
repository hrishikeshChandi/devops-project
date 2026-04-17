import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from main import app
from db.connection import workers_collection

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_worker():
    """Create a worker before running tests"""
    workers_collection.insert_one(
        {
            "worker_id": "test_worker",
            "cpu_cores": 16,
            "ram": 32,
            "available_cpu": 16,
            "available_ram": 32,
            "status": "active",
            "last_heartbeat": datetime.now(timezone.utc),
        }
    )
    yield
    workers_collection.delete_many({"worker_id": "test_worker"})


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
