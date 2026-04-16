from datetime import datetime, timedelta, timezone
from functools import lru_cache
import logging
import random
import threading
import time

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Request, status
from pymongo import ReturnDocument
from pymongo.errors import ConnectionFailure, PyMongoError

from config.constants import HEARTBEAT_TIMEOUT
from db.connection import task_collection, workers_collection
from models.model import StatusUpdate, Task, TaskCreate, TaskResponse
from scheduling.estimator import DurationEstimator
from scheduling.rl_agent import QLearningAgent

from core.metrics import (
    tasks_submitted_total,
    tasks_completed_total,
    tasks_failed_total,
)

router = APIRouter()
estimator = DurationEstimator()
_agent_lock = threading.Lock()
_agent: QLearningAgent | None = None

LOGGER = logging.getLogger("task-router")

MAX_ASSIGNMENT_RETRIES = 4
MAX_COMPLETION_RETRIES = 4
MAX_TASKS_PER_PULL = 4


class _RetryableAssignmentError(RuntimeError):
    pass


def _retry_delay(attempt: int, base: float = 0.05, cap: float = 0.8) -> float:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    return delay + random.uniform(0.0, 0.05)


def _get_agent() -> QLearningAgent:
    global _agent

    if _agent is not None:
        return _agent

    with _agent_lock:
        if _agent is None:
            _agent = QLearningAgent()
        return _agent


def _task_to_response(task: dict) -> TaskResponse:
    payload = dict(task)
    payload["id"] = str(payload.pop("_id"))
    return TaskResponse(**payload)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _heartbeat_cutoff(now: datetime) -> datetime:
    return now - timedelta(seconds=HEARTBEAT_TIMEOUT)


def _is_worker_fresh(worker: dict, now: datetime) -> bool:
    if worker.get("status") != "active":
        return False
    heartbeat = _normalize_datetime(worker.get("last_heartbeat"))
    if heartbeat is None:
        return False
    return heartbeat >= _heartbeat_cutoff(now)


def _mark_worker_dead_if_stale(worker_obj_id: ObjectId, now: datetime):
    workers_collection.update_one(
        {
            "_id": worker_obj_id,
            "status": "active",
            "last_heartbeat": {"$lt": _heartbeat_cutoff(now)},
        },
        {"$set": {"status": "dead"}},
    )


@lru_cache(maxsize=128)
def _estimate_duration(required_cpu: int, required_ram: int) -> float:
    return estimator.estimate(required_cpu, required_ram)


def _compute_rl_metadata(
    available_cpu: int,
    available_ram: int,
    required_cpu: int,
    required_ram: int,
) -> tuple[str, int]:
    agent = _get_agent()
    rl_state = agent.discretize_state(
        worker_cpu=available_cpu,
        worker_ram=available_ram,
        task_cpu=required_cpu,
        task_ram=required_ram,
    )
    rl_action = agent.get_action(rl_state)
    return rl_state, rl_action


def _load_active_worker_snapshot(worker_obj_id: ObjectId, now: datetime) -> dict | None:
    return workers_collection.find_one(
        {
            "_id": worker_obj_id,
            "status": "active",
            "last_heartbeat": {"$gte": _heartbeat_cutoff(now)},
        }
    )


def _claim_next_task(
    worker_id: str,
    now: datetime,
    available_cpu: int,
    available_ram: int,
) -> dict | None:
    # Atomic claim: one task can only transition from pending -> running once.
    # Only claim tasks that this worker can satisfy (prevents thrashing)
    # Also avoid tasks currently in a short cooldown window after recent failed assignments.
    query = {
        "status": "pending",
        "required_cpu": {"$lte": available_cpu},
        "required_ram": {"$lte": available_ram},
        "$or": [
            {"claim_cooldown_until": {"$exists": False}},
            {"claim_cooldown_until": {"$lte": now}},
        ],
    }

    update_pipeline = [
        {
            "$set": {
                "status": "running",
                "worker_id": worker_id,
                "assigned_worker": worker_id,
                "started_at": now,
                "updated_at": now,
                "allocated_cpu": "$required_cpu",
                "allocated_ram": "$required_ram",
                # helpful for debugging/metrics
                "last_claimed_by": worker_id,
            }
        }
    ]

    return task_collection.find_one_and_update(
        query,
        update_pipeline,
        sort=[
            ("priority", -1),
            ("required_cpu", -1),
            ("required_ram", -1),
            ("created_at", 1),
        ],
        return_document=ReturnDocument.AFTER,
    )


def _reserve_worker_resources(
    worker_obj_id: ObjectId,
    task_id: str,
    required_cpu: int,
    required_ram: int,
    now: datetime,
) -> dict | None:
    return workers_collection.find_one_and_update(
        {
            "_id": worker_obj_id,
            "status": "active",
            "last_heartbeat": {"$gte": _heartbeat_cutoff(now)},
            "available_cpu": {"$gte": required_cpu},
            "available_ram": {"$gte": required_ram},
        },
        {
            "$inc": {
                "available_cpu": -required_cpu,
                "available_ram": -required_ram,
            },
            "$addToSet": {"assigned_tasks": task_id},
        },
        return_document=ReturnDocument.AFTER,
    )


def _revert_claimed_task(task_obj_id: ObjectId, worker_id: str, now: datetime) -> None:
    try:
        # When reverting a claim, put the task into a short cooldown and increment
        # a failed assignment counter to reduce immediate re-claiming by other
        # incapable workers.
        cooldown_seconds = 1
        claim_cooldown_until = now + timedelta(seconds=cooldown_seconds)

        task_collection.update_one(
            {
                "_id": task_obj_id,
                "status": "running",
                "assigned_worker": worker_id,
            },
            {
                "$set": {
                    "status": "pending",
                    "worker_id": None,
                    "assigned_worker": None,
                    "started_at": None,
                    "updated_at": now,
                    "allocated_cpu": 0,
                    "allocated_ram": 0,
                    "claim_cooldown_until": claim_cooldown_until,
                },
                "$inc": {"failed_assignment_attempts": 1},
            },
        )
    except PyMongoError as exc:
        LOGGER.warning("Failed to revert claimed task %s: %s", task_obj_id, exc)


def _attach_rl_metadata(
    task: dict,
    available_cpu_before_claim: int,
    available_ram_before_claim: int,
) -> None:
    required_cpu = max(0, int(task.get("required_cpu", 0)))
    required_ram = max(0, int(task.get("required_ram", 0)))
    rl_state, rl_action = _compute_rl_metadata(
        available_cpu=available_cpu_before_claim,
        available_ram=available_ram_before_claim,
        required_cpu=required_cpu,
        required_ram=required_ram,
    )

    try:
        task_collection.update_one(
            {
                "_id": task["_id"],
                "status": "running",
                "assigned_worker": task.get("assigned_worker"),
            },
            {
                "$set": {
                    "rl_state": rl_state,
                    "rl_action_taken": rl_action,
                }
            },
        )
    except PyMongoError as exc:
        LOGGER.warning(
            "Failed to persist RL metadata for task %s: %s", task["_id"], exc
        )

    task["rl_state"] = rl_state
    task["rl_action_taken"] = rl_action


def _assign_tasks_non_transactional(
    worker_obj_id: ObjectId,
    worker_id: str,
    now: datetime,
) -> list[dict]:
    worker = _load_active_worker_snapshot(worker_obj_id, now)
    if not worker:
        return []

    available_cpu = int(worker.get("available_cpu", 0))
    available_ram = int(worker.get("available_ram", 0))

    if available_cpu <= 0 or available_ram <= 0:
        return []

    assigned_tasks: list[dict] = []

    while (
        len(assigned_tasks) < MAX_TASKS_PER_PULL
        and available_cpu > 0
        and available_ram > 0
    ):
        claim_succeeded = False

        for attempt in range(1, MAX_ASSIGNMENT_RETRIES + 1):
            task = _claim_next_task(
                worker_id=worker_id,
                now=now,
                available_cpu=available_cpu,
                available_ram=available_ram,
            )
            if not task:
                return assigned_tasks

            required_cpu = int(task.get("required_cpu", 0))
            required_ram = int(task.get("required_ram", 0))

            if required_cpu > available_cpu or required_ram > available_ram:
                _revert_claimed_task(task["_id"], worker_id, now)
                continue

            required_cpu = max(0, int(task.get("required_cpu", 0)))
            required_ram = max(0, int(task.get("required_ram", 0)))
            if required_cpu <= 0 or required_ram <= 0:
                _revert_claimed_task(task["_id"], worker_id, now)
                time.sleep(0.05)
                continue

            worker_after = _reserve_worker_resources(
                worker_obj_id=worker_obj_id,
                task_id=str(task["_id"]),
                required_cpu=required_cpu,
                required_ram=required_ram,
                now=now,
            )
            if worker_after:
                _attach_rl_metadata(
                    task,
                    available_cpu_before_claim=available_cpu,
                    available_ram_before_claim=available_ram,
                )
                assigned_tasks.append(task)
                available_cpu = int(worker_after.get("available_cpu", 0))
                available_ram = int(worker_after.get("available_ram", 0))
                claim_succeeded = True
                break

            _revert_claimed_task(task["_id"], worker_id, now)
            time.sleep(0.05)

            refreshed = _load_active_worker_snapshot(worker_obj_id, now)
            if not refreshed:
                return assigned_tasks

            available_cpu = int(refreshed.get("available_cpu", 0))
            available_ram = int(refreshed.get("available_ram", 0))

            if available_cpu <= 0 or available_ram <= 0:
                return assigned_tasks

            if attempt < MAX_ASSIGNMENT_RETRIES:
                time.sleep(_retry_delay(attempt))

        if not claim_succeeded:
            break

    return assigned_tasks


@router.get(
    "/get_task",
    response_model=list[TaskResponse],
    status_code=status.HTTP_200_OK,
)
async def get_task(request: Request, worker_id: str):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid worker_id")

    try:
        worker_snapshot = workers_collection.find_one({"_id": worker_obj_id})
    except PyMongoError as exc:
        LOGGER.warning(
            "MongoDB unavailable while loading worker %s: %s", worker_id, exc
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    if not worker_snapshot:
        raise HTTPException(status_code=404, detail="Worker not found")

    now = datetime.now(timezone.utc)
    if not _is_worker_fresh(worker_snapshot, now):

        try:
            _mark_worker_dead_if_stale(worker_obj_id, now)
        except PyMongoError:
            LOGGER.warning("Unable to mark stale worker %s as dead", worker_id)
        return []

    try:
        assigned_tasks = _assign_tasks_non_transactional(
            worker_obj_id=worker_obj_id,
            worker_id=worker_id,
            now=now,
        )
    except ConnectionFailure as exc:
        LOGGER.warning(
            "MongoDB unavailable during assignment for worker %s: %s", worker_id, exc
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )
    except _RetryableAssignmentError as exc:
        LOGGER.warning("Capacity conflict for worker %s: %s", worker_id, exc)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Capacity conflict",
        )
    except PyMongoError as exc:
        LOGGER.warning(
            "MongoDB unavailable during assignment for worker %s: %s", worker_id, exc
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    return [_task_to_response(task) for task in assigned_tasks]


@router.get(
    "/tasks",
    response_model=list[TaskResponse],
    status_code=status.HTTP_200_OK,
)
async def get_tasks(request: Request):
    try:
        tasks = task_collection.find({"status": "pending"})
        return [_task_to_response(task) for task in tasks]
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while listing pending tasks: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )


@router.get(
    "/all_tasks",
    response_model=list[TaskResponse],
    status_code=status.HTTP_200_OK,
)
async def get_all_tasks(request: Request):
    try:
        tasks = task_collection.find().sort("created_at", -1).limit(100)
        return [_task_to_response(task) for task in tasks]
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while listing all tasks: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )


# @router.post(
#     "/submit",
#     response_model=TaskResponse,
#     status_code=status.HTTP_201_CREATED,
# )
# async def submit_task(request: Request, inputTask: TaskCreate):
#     try:
#         # capable_worker = workers_collection.find_one(
#         #     {
#         #         "cpu_cores": {"$gte": inputTask.required_cpu},
#         #         "ram": {"$gte": inputTask.required_ram},
#         #     }
#         # )
#         capable_worker = workers_collection.find_one(
#             {
#                 "available_cpu": {"$gte": inputTask.required_cpu},
#                 "available_ram": {"$gte": inputTask.required_ram},
#                 "status": "active",
#             }
#         )
#         if not capable_worker:
#             raise HTTPException(
#                 status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
#                 detail=(
#                     f"No worker exists with at least {inputTask.required_cpu} CPU "
#                     f"and {inputTask.required_ram} RAM. Task would never be scheduled."
#                 ),
#             )

#         task = Task(
#             data=inputTask.data,
#             required_cpu=inputTask.required_cpu,
#             required_ram=inputTask.required_ram,
#             priority=inputTask.priority,
#         )
#         result = task_collection.insert_one(task.model_dump())
#         return TaskResponse(
#             id=str(result.inserted_id),
#             **task.model_dump(),
#         )
#     except HTTPException:
#         raise
#     except PyMongoError as exc:
#         LOGGER.warning("MongoDB unavailable while submitting task: %s", exc)
#         raise HTTPException(
#             status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#             detail="Database unavailable",
#         )
#     except Exception as exc:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error submitting task ({str(exc)})",
#         )


@router.post(
    "/submit",
    response_model=TaskResponse,
    status_code=status.HTTP_201_CREATED,
)
async def submit_task(request: Request, inputTask: TaskCreate):
    try:
        capable_worker = workers_collection.find_one(
            {
                "available_cpu": {"$gte": inputTask.required_cpu},
                "available_ram": {"$gte": inputTask.required_ram},
                "status": "active",
            }
        )

        if not capable_worker:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"No worker exists with at least {inputTask.required_cpu} CPU "
                    f"and {inputTask.required_ram} RAM. Task would never be scheduled."
                ),
            )

        task = Task(
            data=inputTask.data,
            required_cpu=inputTask.required_cpu,
            required_ram=inputTask.required_ram,
            priority=inputTask.priority,
        )

        result = task_collection.insert_one(task.model_dump())

        # ✅ PROMETHEUS — TASK SUBMITTED
        tasks_submitted_total.inc()

        return TaskResponse(
            id=str(result.inserted_id),
            **task.model_dump(),
        )

    except HTTPException:
        raise

    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while submitting task: %s", exc)

        # ✅ PROMETHEUS — FAILED
        tasks_failed_total.inc()

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    except Exception as exc:

        # ✅ PROMETHEUS — FAILED
        tasks_failed_total.inc()

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error submitting task ({str(exc)})",
        )


def _finalize_task_non_transactional(
    task_obj_id: ObjectId,
    task_id: str,
    final_status: str,
) -> dict:
    for attempt in range(1, MAX_COMPLETION_RETRIES + 1):
        now = datetime.now(timezone.utc)

        task = task_collection.find_one({"_id": task_obj_id})
        if not task:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Task not found",
            )

        current_status = str(task.get("status", "")).lower()
        if current_status == "running":
            transitioned = task_collection.find_one_and_update(
                {
                    "_id": task_obj_id,
                    "status": "running",
                },
                {
                    "$set": {
                        "status": final_status,
                        "updated_at": now,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
            if not transitioned:
                if attempt < MAX_COMPLETION_RETRIES:
                    time.sleep(_retry_delay(attempt))
                    continue
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Task status changed concurrently",
                )
            task = transitioned
        elif current_status in {"completed", "failed"}:
            if current_status != final_status:
                LOGGER.info(
                    "Task %s already finalized as %s (requested=%s)",
                    task_id,
                    current_status,
                    final_status,
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Task is not in a finalizable state",
            )

        worker_id = task.get("assigned_worker")
        allocated_cpu = max(0, int(task.get("allocated_cpu", 0)))
        allocated_ram = max(0, int(task.get("allocated_ram", 0)))

        if worker_id and (allocated_cpu > 0 or allocated_ram > 0):
            worker_obj_id = None
            try:
                worker_obj_id = ObjectId(worker_id)
            except Exception:
                LOGGER.warning(
                    "Task %s has non-ObjectId worker reference %s", task_id, worker_id
                )

            if worker_obj_id is not None:
                workers_result = workers_collection.update_one(
                    {
                        "_id": worker_obj_id,
                        "assigned_tasks": task_id,
                    },
                    {
                        "$inc": {
                            "available_cpu": allocated_cpu,
                            "available_ram": allocated_ram,
                        },
                        "$pull": {"assigned_tasks": task_id},
                    },
                )
                if workers_result.modified_count == 0:
                    LOGGER.info(
                        "Task %s release was already applied or worker missing task reference",
                        task_id,
                    )

        updated_task = task_collection.find_one_and_update(
            {"_id": task_obj_id},
            {
                "$set": {
                    "worker_id": None,
                    "completed_by": worker_id,  # ✅ ADD THIS
                    # REMOVE THIS LINE ↓
                    # "assigned_worker": None,
                    "allocated_cpu": 0,
                    "allocated_ram": 0,
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated_task:
            return updated_task

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to finalize task",
    )


# @router.put(
#     "/update_status/{task_id}",
#     response_model=TaskResponse,
#     status_code=status.HTTP_200_OK,
# )
# async def update_task(request: Request, task_id: str, update: StatusUpdate):
#     final_status = update.status.lower()
#     if final_status not in ["completed", "failed"]:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Only 'completed' or 'failed' are allowed",
#         )

#     try:
#         task_obj_id = ObjectId(task_id)
#     except Exception:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Invalid task id",
#         )

#     for attempt in range(1, MAX_COMPLETION_RETRIES + 1):
#         try:
#             updated_task = _finalize_task_non_transactional(
#                 task_obj_id=task_obj_id,
#                 task_id=task_id,
#                 final_status=final_status,
#             )
#             return _task_to_response(updated_task)
#         except HTTPException:
#             raise
#         except ConnectionFailure as exc:
#             if attempt < MAX_COMPLETION_RETRIES:
#                 LOGGER.warning(
#                     "Task finalization retry for %s after connection failure: %s",
#                     task_id,
#                     exc,
#                 )
#                 time.sleep(_retry_delay(attempt))
#                 continue
#             raise HTTPException(
#                 status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#                 detail="Database unavailable",
#             )
#         except PyMongoError as exc:
#             if attempt < MAX_COMPLETION_RETRIES:
#                 LOGGER.warning(
#                     "Task finalization retry for %s after DB error: %s",
#                     task_id,
#                     exc,
#                 )
#                 time.sleep(_retry_delay(attempt))
#                 continue
#             LOGGER.exception("Task finalization failed for %s: %s", task_id, exc)
#             raise HTTPException(
#                 status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
#                 detail="Database unavailable",
#             )

#     raise HTTPException(
#         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         detail="Failed to update task status",
#     )


@router.put(
    "/update_status/{task_id}",
    response_model=TaskResponse,
    status_code=status.HTTP_200_OK,
)
async def update_task(request: Request, task_id: str, update: StatusUpdate):
    final_status = update.status.lower()

    if final_status not in ["completed", "failed"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only 'completed' or 'failed' are allowed",
        )

    try:
        task_obj_id = ObjectId(task_id)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task id",
        )

    for attempt in range(1, MAX_COMPLETION_RETRIES + 1):
        try:
            updated_task = _finalize_task_non_transactional(
                task_obj_id=task_obj_id,
                task_id=task_id,
                final_status=final_status,
            )

            # previous_status = updated_task.get("status")

            # if previous_status == "completed":
            #     tasks_completed_total.inc()
            # elif previous_status == "failed":
            #     tasks_failed_total.inc()
            if final_status == "completed":
                tasks_completed_total.inc()
            elif final_status == "failed":
                tasks_failed_total.inc()

            return _task_to_response(updated_task)

        except HTTPException:
            raise

        except ConnectionFailure as exc:
            if attempt < MAX_COMPLETION_RETRIES:
                LOGGER.warning(
                    "Task finalization retry for %s after connection failure: %s",
                    task_id,
                    exc,
                )
                time.sleep(_retry_delay(attempt))
                continue
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database unavailable",
            )

        except PyMongoError as exc:
            if attempt < MAX_COMPLETION_RETRIES:
                LOGGER.warning(
                    "Task finalization retry for %s after DB error: %s",
                    task_id,
                    exc,
                )
                time.sleep(_retry_delay(attempt))
                continue

            LOGGER.exception("Task finalization failed for %s: %s", task_id, exc)

            # ✅ PROMETHEUS — FAILED
            tasks_failed_total.inc()

            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database unavailable",
            )

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to update task status",
    )
