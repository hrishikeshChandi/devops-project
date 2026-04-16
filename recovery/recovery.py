import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import random
from datetime import datetime, timezone
from bson import ObjectId
from db.connection import task_collection, workers_collection
from config.constants import TIMEOUT, MAX_RETRIES
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

LOGGER = logging.getLogger("recovery-daemon")


def _release_worker_resources(
    worker_id: str, allocated_cpu: int, allocated_ram: int, task_id
):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        worker_obj_id = worker_id

    try:
        workers_collection.update_one(
            {"_id": worker_obj_id},
            {
                "$inc": {
                    "available_cpu": allocated_cpu,
                    "available_ram": allocated_ram,
                },
                "$pull": {"assigned_tasks": str(task_id)},
            },
        )
    except PyMongoError as exc:
        LOGGER.warning(
            "MongoDB unavailable while releasing worker resources for %s: %s",
            worker_id,
            exc,
        )


def run():
    print("Recovery daemon started")

    while True:
        try:
            running_tasks = task_collection.find({"status": "running"})
            now = datetime.now(timezone.utc)

            for task in running_tasks:
                task_id = task["_id"]
                started_at = task.get("started_at")
                allocated_cpu = task.get("allocated_cpu", 0)
                allocated_ram = task.get("allocated_ram", 0)
                worker_id = task.get("assigned_worker")

                if not started_at:
                    continue

                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)

                elapsed_seconds = (now - started_at).total_seconds()

                if elapsed_seconds <= TIMEOUT:
                    continue

                current_retries = task.get("retry_count", 0)

                if worker_id and (allocated_cpu > 0 or allocated_ram > 0):
                    _release_worker_resources(
                        worker_id, allocated_cpu, allocated_ram, task_id
                    )

                if current_retries < MAX_RETRIES:
                    retries_left = MAX_RETRIES - (current_retries + 1)
                    print(f"Task {task_id} → pending (retries left: {retries_left})")

                    task_collection.update_one(
                        {"_id": task_id},
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
                else:
                    print(f"Task {task_id} → FAILED (retry limit exceeded)")

                    task_collection.update_one(
                        {"_id": task_id},
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
            LOGGER.warning("MongoDB unavailable in recovery loop: %s", exc)

        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    run()
