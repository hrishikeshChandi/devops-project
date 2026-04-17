import logging
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, HTTPException, status
from pymongo.errors import PyMongoError

from db.connection import q_table_collection, task_collection, task_history_collection
from scheduling.estimator import DurationEstimator

router = APIRouter()
estimator = DurationEstimator()
LOGGER = logging.getLogger("learning-router")


@router.post("/update")
async def update_learning(
    task_id: str,
    success: bool,
    actual_duration: float,
    scheduling_valid: bool = True,
):
    try:
        obj_id = ObjectId(task_id)
    except Exception:
        return {"error": "Invalid task_id"}

    try:
        task = task_collection.find_one({"_id": obj_id})
        if not task:
            return {"error": "Task not found"}

        est_duration = estimator.estimate(
            task.get("required_cpu", 0), task.get("required_ram", 0)
        )

        history_entry = {
            "task_id": task_id,
            "success": success,
            "actual_duration": actual_duration,
            "scheduling_valid": scheduling_valid,
            "estimated_duration": est_duration,
            "required_cpu": task.get("required_cpu"),
            "required_ram": task.get("required_ram"),
            "cpu_bin": estimator.get_cpu_bin(task.get("required_cpu", 0)),
            "ram_bin": estimator.get_ram_bin(task.get("required_ram", 0)),
            "rl_state": task.get("rl_state"),
            "rl_action_taken": task.get("rl_action_taken"),
            "timestamp": datetime.now(timezone.utc),
            "processed_by_learner": False,
        }
        task_history_collection.insert_one(history_entry)

        task_collection.update_one(
            {"_id": obj_id},
            {
                "$set": {
                    "rl_outcome": {
                        "success": success,
                        "duration": actual_duration,
                    }
                }
            },
        )
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while updating learning: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    return {"status": "success"}


@router.get("/metrics/rl")
async def get_rl_metrics():
    try:
        total_samples = task_history_collection.count_documents({})
        completed_samples = task_history_collection.count_documents({"success": True})

        q_entries = list(q_table_collection.find().sort("values.0", -1).limit(10))

        processed_q = []
        for entry in q_entries:
            vals = entry["values"]
            diff = abs(vals[0] - vals[1])
            processed_q.append(
                {
                    "state": entry["state"],
                    "q_values": vals,
                    "diff": diff,
                }
            )

        processed_q.sort(key=lambda x: x["diff"], reverse=True)
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while reading RL metrics: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    return {
        "total_updates": total_samples,
        "success_rate": (completed_samples / total_samples) if total_samples > 0 else 0,
        "q_table_sample": processed_q[:5],
    }
