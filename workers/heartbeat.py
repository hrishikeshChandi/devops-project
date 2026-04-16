import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from bson import ObjectId
from typing import List
from datetime import datetime, timezone
from db.connection import workers_collection, task_collection
from config.constants import MAX_RETRIES, HEARTBEAT_TIMEOUT
from pymongo.errors import PyMongoError

import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    )
    logger.addHandler(handler)

logger.setLevel(logging.INFO)

LOGGER = logging.getLogger("heartbeat-monitor")


def free_resources(worker_id: str, assigned_tasks: List[str], now: datetime):
    for task_id in assigned_tasks:
        try:
            obj_id = ObjectId(task_id)
        except Exception:
            continue

        try:
            task = task_collection.find_one({"_id": obj_id})
        except PyMongoError as exc:
            LOGGER.warning(
                "MongoDB unavailable while loading task %s: %s", task_id, exc
            )
            continue

        # ✅ extra safety: ensure task still belongs to this worker
        if not task or task.get("status") != "running":
            continue

        if str(task.get("assigned_worker")) != worker_id:
            continue

        cpu = task.get("allocated_cpu", 0)
        ram = task.get("allocated_ram", 0)
        current_retries = task.get("retry_count", 0)

        try:
            workers_collection.update_one(
                {"_id": ObjectId(worker_id)},
                {
                    "$inc": {"available_cpu": cpu, "available_ram": ram},
                    "$pull": {"assigned_tasks": str(task_id)},
                },
            )
        except PyMongoError as exc:
            LOGGER.warning(
                "MongoDB unavailable while releasing resources for worker %s: %s",
                worker_id,
                exc,
            )
            continue

        if current_retries < MAX_RETRIES:
            try:
                task_collection.update_one(
                    {"_id": obj_id},
                    {
                        "$set": {
                            "status": "pending",
                            "assigned_worker": None,
                            "started_at": None,
                            "updated_at": now,
                            "allocated_cpu": 0,
                            "allocated_ram": 0,
                        },
                        "$inc": {"retry_count": 1},
                    },
                )
            except PyMongoError as exc:
                LOGGER.warning(
                    "MongoDB unavailable while retrying task %s: %s", task_id, exc
                )
        else:
            try:
                task_collection.update_one(
                    {"_id": obj_id},
                    {
                        "$set": {
                            "status": "failed",
                            "assigned_worker": None,
                            "updated_at": now,
                            "allocated_cpu": 0,
                            "allocated_ram": 0,
                        }
                    },
                )
            except PyMongoError as exc:
                LOGGER.warning(
                    "MongoDB unavailable while failing task %s: %s", task_id, exc
                )


def run():
    print("Heartbeat monitor started")

    while True:
        try:
            workers = workers_collection.find({"status": "active"})
            now = datetime.now(timezone.utc)

            for worker in workers:
                last_heartbeat = worker.get("last_heartbeat")

                if not last_heartbeat:
                    continue

                if last_heartbeat.tzinfo is None:
                    last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)

                gap = (now - last_heartbeat).total_seconds()

                if gap > HEARTBEAT_TIMEOUT:
                    result = workers_collection.update_one(
                        {"_id": worker["_id"], "status": "active"},
                        {"$set": {"status": "dead"}},
                    )

                    if result.modified_count == 0:
                        continue

                    print(f"Worker {worker['_id']} DEAD (gap {gap:.0f}s)")

                    free_resources(
                        worker_id=str(worker["_id"]),
                        assigned_tasks=worker.get("assigned_tasks", []),
                        now=now,
                    )
        except PyMongoError as exc:
            LOGGER.warning("MongoDB unavailable in heartbeat loop: %s", exc)

        time.sleep(10)


if __name__ == "__main__":
    run()
