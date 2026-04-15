from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from typing import TypeVar

from pymongo import MongoClient
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    PyMongoError,
    ServerSelectionTimeoutError,
)

from config.constants import (
    MONGO_DB_NAME,
    MONGO_RETRY_BASE_SECONDS,
    MONGO_RETRY_MAX_SECONDS,
)

LOGGER = logging.getLogger("db-connection")

RETRYABLE_ERRORS = (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)

T = TypeVar("T")

_CLIENT_LOCK = threading.RLock()
_CLIENT: MongoClient | None = None

_INDEX_LOCK = threading.Lock()
_INDEXES_READY = False


# Hard-coded standalone URI to prevent any replica-set misconfiguration.
_STANDALONE_URI = "mongodb://127.0.0.1:27017"


def _build_client() -> MongoClient:
    return MongoClient(_STANDALONE_URI)


def get_client() -> MongoClient:
    global _CLIENT

    with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = _build_client()
            LOGGER.info(
                "MongoClient initialized for standalone MongoDB at %s", _STANDALONE_URI
            )
        return _CLIENT


def get_database():
    return get_client()[MONGO_DB_NAME]


class MongoClientProxy:
    def __getattr__(self, item):
        return getattr(get_client(), item)


class MongoDatabaseProxy:
    def __getattr__(self, item):
        return getattr(get_database(), item)

    def __getitem__(self, item):
        return get_database()[item]


class MongoCollectionProxy:
    def __init__(self, collection_name: str):
        self._collection_name = collection_name

    def _collection(self):
        return get_database()[self._collection_name]

    def __getattr__(self, item):
        return getattr(self._collection(), item)


client = MongoClientProxy()
db = MongoDatabaseProxy()

task_collection = MongoCollectionProxy("tasks")
workers_collection = MongoCollectionProxy("workers")
task_history_collection = MongoCollectionProxy("task_history")
q_table_collection = MongoCollectionProxy("q_table")


def run_with_mongo_retry(
    operation: Callable[[], T],
    operation_name: str,
    max_attempts: int = 3,
    base_delay_seconds: float = MONGO_RETRY_BASE_SECONDS,
    max_delay_seconds: float = MONGO_RETRY_MAX_SECONDS,
) -> T:
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except RETRYABLE_ERRORS as exc:
            if attempt == max_attempts:
                LOGGER.error(
                    "Mongo retry exhausted for %s after %s attempts: %s",
                    operation_name,
                    attempt,
                    exc,
                )
                raise

            delay = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
            delay += random.uniform(0.0, 0.15)
            LOGGER.warning(
                "Mongo operation %s failed on attempt %s/%s: %s. Retrying in %.2fs",
                operation_name,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            time.sleep(delay)
        except PyMongoError:
            raise


def ping() -> bool:
    try:
        get_client().admin.command("ping")
        return True
    except RETRYABLE_ERRORS:
        return False


def ensure_indexes_once() -> bool:
    global _INDEXES_READY

    if _INDEXES_READY:
        return True

    with _INDEX_LOCK:
        if _INDEXES_READY:
            return True

        try:
            task_collection.create_index(
                [
                    ("status", 1),
                    ("priority", -1),
                    ("created_at", 1),
                ],
                name="task_pending_priority_created_idx",
            )
            task_collection.create_index(
                [
                    ("status", 1),
                    ("required_cpu", 1),
                    ("required_ram", 1),
                ],
                name="task_pending_resources_idx",
            )

            # _id is indexed by MongoDB automatically; no separate manual index needed.
            workers_collection.create_index(
                [("worker_id", 1)],
                name="worker_id_idx",
            )
            workers_collection.create_index(
                [("available_cpu", 1)],
                name="worker_available_cpu_idx",
            )
            workers_collection.create_index(
                [("available_ram", 1)],
                name="worker_available_ram_idx",
            )
            workers_collection.create_index(
                [("status", 1), ("last_heartbeat", -1)],
                name="worker_status_heartbeat_idx",
            )

            task_history_collection.create_index(
                [("processed_by_learner", 1), ("timestamp", 1)],
                name="learner_pending_idx",
            )
            q_table_collection.create_index(
                [("state", 1)],
                unique=True,
                name="q_table_state_unique_idx",
            )

            _INDEXES_READY = True
            LOGGER.info("Mongo indexes ensured")
            return True
        except PyMongoError as exc:
            LOGGER.warning("Skipping Mongo index bootstrap: %s", exc)
            return False


def initialize_mongo() -> None:
    # Simple startup behavior: try connecting once, log failures, continue.
    try:
        get_client().admin.command("ping")
        LOGGER.info("MongoDB connection successful")
    except PyMongoError as exc:
        LOGGER.error("MongoDB connection failed: %s", exc)
        return

    ensure_indexes_once()
