from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from routers import task, workers, learning
from core.limiter import limiter
import uvicorn
import logging

from slowapi.middleware import SlowAPIMiddleware
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

from config.constants import HOST, MODULE, PORT, RL_ENABLED
from scheduling.learner import start_learner
from db.connection import initialize_mongo

from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger import jsonlogger

from core.metrics import (
    pending_tasks_gauge,
    active_workers_gauge,
    rl_success_rate_gauge,
)

logger = logging.getLogger()

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.setLevel(logging.INFO)

LOGGER = logging.getLogger("app-startup")

app = FastAPI()

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(task.router, prefix="/tasks", tags=["tasks"])
app.include_router(workers.router, prefix="/workers", tags=["workers"])
app.include_router(learning.router, prefix="/learning", tags=["learning"])


@app.on_event("startup")
async def startup_event():
    initialize_mongo()

    if RL_ENABLED:
        start_learner()
        LOGGER.info("RL background learner started")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    from db.connection import task_collection, workers_collection

    pending = task_collection.count_documents({"status": "pending"})
    active_workers = workers_collection.count_documents({"status": "active"})

    pending_tasks_gauge.set(pending)
    active_workers_gauge.set(active_workers)

    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run(
        MODULE,
        host=HOST,
        port=PORT,
        reload=True,
    )
