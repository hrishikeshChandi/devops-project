from fastapi import FastAPI
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


if __name__ == "__main__":
    uvicorn.run(
        MODULE,
        host=HOST,
        port=PORT,
        reload=True,
    )
