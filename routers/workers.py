import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Request, status
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from db.connection import workers_collection
from models.model import Worker, WorkerCreate, WorkerResponse

router = APIRouter()
LOGGER = logging.getLogger("workers-router")


def _spawn_worker_subprocess(worker_id: str) -> subprocess.Popen:
    workspace_root = Path(__file__).resolve().parent.parent
    command = [sys.executable, "-m", "workers.worker", "--worker-id", worker_id]

    kwargs = {
        "cwd": str(workspace_root),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }

    if os.name == "nt":
        creationflags = 0
        creationflags |= getattr(subprocess, "DETACHED_PROCESS", 0)
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        kwargs["creationflags"] = creationflags
    else:
        kwargs["start_new_session"] = True

    return subprocess.Popen(command, **kwargs)


@router.get(
    "/get_workers",
    response_model=list[WorkerResponse],
    status_code=status.HTTP_200_OK,
)
async def get_workers(request: Request):
    try:
        workers = workers_collection.find()
        result = []
        for worker in workers:
            worker["id"] = str(worker["_id"])
            del worker["_id"]
            result.append(WorkerResponse(**worker))
        return result
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while listing workers: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )


@router.get(
    "/worker/{worker_id}",
    response_model=WorkerResponse,
    status_code=status.HTTP_200_OK,
)
async def get_worker(request: Request, worker_id: str):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid worker_id")

    try:
        worker = workers_collection.find_one({"_id": worker_obj_id})
    except PyMongoError as exc:
        LOGGER.warning(
            "MongoDB unavailable while loading worker %s: %s", worker_id, exc
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")

    worker["id"] = str(worker["_id"])
    del worker["_id"]
    return WorkerResponse(**worker)


@router.post(
    "/add_worker",
    response_model=WorkerResponse,
    status_code=status.HTTP_201_CREATED,
)
async def add_worker(request: Request, worker: WorkerCreate):
    try:
        worker_obj = Worker(
            cpu_cores=worker.cpu_cores,
            ram=worker.ram,
            available_cpu=worker.cpu_cores,
            available_ram=worker.ram,
        )

        result = workers_collection.insert_one(worker_obj.model_dump())
        worker_id = str(result.inserted_id)

        workers_collection.update_one(
            {"_id": result.inserted_id},
            {"$set": {"worker_id": worker_id}},
        )

        if worker.simulate and os.getenv("DISABLE_WORKER_SPAWN") != "true":
            try:
                process = _spawn_worker_subprocess(worker_id)
                workers_collection.update_one(
                    {"_id": result.inserted_id},
                    {
                        "$set": {
                            "runtime_mode": "subprocess",
                            "runtime_pid": process.pid,
                        }
                    },
                )
                LOGGER.info(
                    "Spawned subprocess worker %s with pid=%s", worker_id, process.pid
                )
            except Exception as exc:
                workers_collection.update_one(
                    {"_id": result.inserted_id},
                    {"$set": {"status": "dead"}},
                )
                LOGGER.exception("Failed to start worker subprocess for %s", worker_id)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Worker registered but runtime failed to start ({exc})",
                )

        return WorkerResponse(
            id=worker_id,
            **worker_obj.model_dump(),
        )

    except HTTPException:
        raise
    except PyMongoError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database unavailable ({str(exc)})",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error adding worker ({str(exc)})",
        )


@router.put("/heartbeat/{worker_id}", status_code=status.HTTP_200_OK)
async def update_heartbeat(request: Request, worker_id: str):
    try:
        worker_obj_id = ObjectId(worker_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid worker_id")

    now = datetime.now(timezone.utc)

    try:
        worker = workers_collection.find_one_and_update(
            {"_id": worker_obj_id},
            {"$set": {"last_heartbeat": now, "status": "active"}},
            return_document=ReturnDocument.AFTER,
        )
    except PyMongoError as exc:
        LOGGER.warning("MongoDB unavailable while processing heartbeat: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )

    if not worker:
        raise HTTPException(
            status_code=404,
            detail=f"Worker with id {worker_id} does not exist",
        )

    return {"message": "heartbeat updated"}
