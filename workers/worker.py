import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import random
import threading
import time
from typing import Optional

import requests
from pythonjsonlogger import jsonlogger

from config.constants import HEARTBEAT_INTERVAL, HOST, PORT

logger = logging.getLogger(__name__)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        jsonlogger.JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    )
    logger.addHandler(handler)

logger.setLevel(logging.INFO)

TASK_URL = f"http://{HOST}:{PORT}/tasks"
WORKER_URL = f"http://{HOST}:{PORT}/workers"
LEARNING_URL = f"http://{HOST}:{PORT}/learning/update"

LOGGER = logging.getLogger("worker-runtime")


def _backoff_delay(attempt: int, base: float = 0.5, cap: float = 8.0) -> float:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    return delay + random.uniform(0.0, 0.15)


def execute_task(task: dict) -> bool:
    cpu = max(1, int(task.get("required_cpu", 1)))
    ram = max(1, int(task.get("required_ram", 1)))

    duration = min(3.0, 0.05 * cpu + 0.03 * ram)
    time.sleep(duration)

    # Controlled failure rate for realistic instability without random runtime bugs.
    if random.random() < 0.3:
        raise Exception("Simulated execution failure")

    return True


def _request_once(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: float,
    **kwargs,
) -> requests.Response:
    return session.request(method=method, url=url, timeout=timeout, **kwargs)


def register_worker(simulate: bool = False, max_attempts: Optional[int] = None):
    ram_options = [4, 6, 8, 12, 16, 24, 32, 48]
    cpu_options = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

    cpu = random.choice(cpu_options)
    ram = random.choice(ram_options)

    attempt = 0
    retryable_codes = {408, 425, 429, 500, 502, 503, 504}

    with requests.Session() as session:
        while max_attempts is None or attempt < max_attempts:
            attempt += 1
            try:
                response = _request_once(
                    session,
                    "POST",
                    f"{WORKER_URL}/add_worker",
                    json={"cpu_cores": cpu, "ram": ram, "simulate": simulate},
                    timeout=5,
                )

                if response.status_code == 201:
                    worker_id = response.json()["id"]
                    LOGGER.info(
                        "Registered worker %s with CPU=%s RAM=%s", worker_id, cpu, ram
                    )
                    return worker_id, cpu, ram

                if response.status_code not in retryable_codes:
                    raise RuntimeError(
                        f"Failed to register worker (HTTP {response.status_code}): {response.text}"
                    )

                LOGGER.warning(
                    "Worker registration retryable HTTP %s (attempt %s)",
                    response.status_code,
                    attempt,
                )
            except requests.RequestException as exc:
                LOGGER.warning(
                    "Worker registration network error on attempt %s: %s", attempt, exc
                )

            delay = _backoff_delay(attempt)
            time.sleep(delay)

    raise RuntimeError("Failed to register worker after retries")


def load_existing_worker(worker_id: str, max_attempts: int = 5) -> tuple[int, int]:
    retryable_codes = {408, 425, 429, 500, 502, 503, 504}

    with requests.Session() as session:
        for attempt in range(1, max_attempts + 1):
            try:
                response = _request_once(
                    session,
                    "GET",
                    f"{WORKER_URL}/worker/{worker_id}",
                    timeout=5,
                )
                if response.status_code == 200:
                    payload = response.json()
                    return int(payload["cpu_cores"]), int(payload["ram"])

                if response.status_code == 404:
                    raise RuntimeError(f"Worker {worker_id} not found")

                if response.status_code not in retryable_codes:
                    raise RuntimeError(
                        f"Unable to load worker {worker_id} (HTTP {response.status_code}): {response.text}"
                    )

                LOGGER.warning(
                    "Retryable response while loading worker %s: HTTP %s",
                    worker_id,
                    response.status_code,
                )
            except requests.RequestException as exc:
                LOGGER.warning(
                    "Failed to load worker %s on attempt %s: %s",
                    worker_id,
                    attempt,
                    exc,
                )

            if attempt < max_attempts:
                time.sleep(_backoff_delay(attempt, base=0.2, cap=2.0))

    raise RuntimeError(f"Failed to load worker {worker_id} after retries")


class WorkerRuntime:
    def __init__(self, worker_id: str, total_cpu: int, total_ram: int, label: str):
        self.worker_id = worker_id
        self.total_cpu = total_cpu
        self.total_ram = total_ram
        self.label = label

        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._session = requests.Session()

        self._inflight_cpu = 0
        self._inflight_ram = 0
        self._inflight_tasks: dict[str, tuple[int, int]] = {}

        self._initial_backoff = 0.5
        self._max_backoff = 2.0
        self._max_poll_sleep = 1.0
        self._poll_backoff = self._initial_backoff
        self._had_capacity = True

    def _prefix(self) -> str:
        return f"[{self.label} {self.worker_id[-6:]}]"

    def stop(self):
        self._stop_event.set()

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        attempts: int = 3,
        timeout: float = 4.0,
        **kwargs,
    ) -> Optional[requests.Response]:
        retryable_codes = {408, 425, 429, 500, 502, 503, 504}

        for attempt in range(1, attempts + 1):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    timeout=timeout,
                    **kwargs,
                )
                if response.status_code in retryable_codes and attempt < attempts:
                    time.sleep(_backoff_delay(attempt, base=0.2, cap=2.0))
                    continue
                return response
            except requests.RequestException as exc:
                if attempt == attempts:
                    LOGGER.warning(
                        "%s request failed %s %s: %s", self._prefix(), method, url, exc
                    )
                    return None
                time.sleep(_backoff_delay(attempt, base=0.2, cap=2.0))
        return None

    def _heartbeat_loop(self):
        while not self._stop_event.is_set():
            response = self._request_with_retry(
                "PUT",
                f"{WORKER_URL}/heartbeat/{self.worker_id}",
                attempts=2,
                timeout=2.5,
            )
            if response is None:
                LOGGER.warning("%s heartbeat failed after retries", self._prefix())
            elif response.status_code != 200:
                LOGGER.warning(
                    "%s heartbeat rejected with status %s",
                    self._prefix(),
                    response.status_code,
                )
            self._stop_event.wait(HEARTBEAT_INTERVAL)

    def _update_status(self, task_id: str, status: str) -> bool:
        response = self._request_with_retry(
            "PUT",
            f"{TASK_URL}/update_status/{task_id}",
            json={"status": status},
            attempts=3,
            timeout=4,
        )
        if response is None:
            return False
        if response.status_code != 200:
            LOGGER.warning(
                "%s update_status failed for task %s: HTTP %s",
                self._prefix(),
                task_id,
                response.status_code,
            )
            return False
        return True

    def _post_learning(
        self,
        task_id: str,
        success: bool,
        duration: float,
        scheduling_valid: bool,
    ):
        response = self._request_with_retry(
            "POST",
            LEARNING_URL,
            params={
                "task_id": task_id,
                "success": success,
                "actual_duration": duration,
                "scheduling_valid": scheduling_valid,
            },
            attempts=3,
            timeout=4,
        )
        if response is None:
            LOGGER.warning(
                "%s learning update failed for task %s", self._prefix(), task_id
            )
            return
        if response.status_code != 200:
            LOGGER.warning(
                "%s learning update rejected for task %s: HTTP %s",
                self._prefix(),
                task_id,
                response.status_code,
            )

    def _release_local_capacity(self, task_id: str):
        with self._lock:
            cpu_ram = self._inflight_tasks.pop(task_id, None)
            if not cpu_ram:
                return
            cpu, ram = cpu_ram
            self._inflight_cpu = max(0, self._inflight_cpu - cpu)
            self._inflight_ram = max(0, self._inflight_ram - ram)

    def _bounded_poll_sleep(self):
        self._stop_event.wait(min(self._poll_backoff, self._max_poll_sleep))

    def _on_task_found(self):
        self._poll_backoff = self._initial_backoff

    def _on_no_task_found(self):
        self._poll_backoff = min(self._poll_backoff * 2, self._max_backoff)

    def _process_task(self, task: dict):
        task_id = str(task.get("id", ""))
        start_time = time.time()
        final_status = "failed"

        cpu = max(1, int(task.get("allocated_cpu") or task.get("required_cpu", 1)))
        ram = max(1, int(task.get("allocated_ram") or task.get("required_ram", 1)))
        scheduling_valid = cpu <= self.total_cpu and ram <= self.total_ram

        try:
            execute_task(task)
            final_status = "completed"
            print(f"[Worker {self.worker_id}] completed task {task_id}")
        except Exception as exc:
            final_status = "failed"
            print(f"[Worker {self.worker_id}] failed task {task_id}")
            LOGGER.warning(
                "%s task execution failure for %s: %s", self._prefix(), task_id, exc
            )
        finally:
            duration = time.time() - start_time
            if not self._update_status(task_id, final_status):
                LOGGER.warning(
                    "%s could not finalize task %s with status=%s",
                    self._prefix(),
                    task_id,
                    final_status,
                )
            self._post_learning(
                task_id,
                final_status == "completed",
                duration,
                scheduling_valid,
            )
            self._release_local_capacity(task_id)

    def _try_start_task(self, task: dict) -> bool:
        task_id = str(task.get("id", ""))
        if not task_id:
            return False

        cpu = int(task.get("allocated_cpu") or task.get("required_cpu", 0))
        ram = int(task.get("allocated_ram") or task.get("required_ram", 0))
        if cpu <= 0 or ram <= 0:
            return False

        with self._lock:
            if self._inflight_cpu + cpu > self.total_cpu:
                return False
            if self._inflight_ram + ram > self.total_ram:
                return False
            if task_id in self._inflight_tasks:
                return False
            self._inflight_cpu += cpu
            self._inflight_ram += ram
            self._inflight_tasks[task_id] = (cpu, ram)

        threading.Thread(
            target=self._process_task,
            args=(task,),
            daemon=True,
            name=f"worker-task-{task_id[-6:]}",
        ).start()
        return True

    def _remaining_capacity(self) -> tuple[int, int]:
        with self._lock:
            return (
                max(0, self.total_cpu - self._inflight_cpu),
                max(0, self.total_ram - self._inflight_ram),
            )

    def _can_pull(self) -> bool:
        remaining_cpu, remaining_ram = self._remaining_capacity()
        return remaining_cpu >= 1 and remaining_ram >= 1

    def run_forever(self):
        LOGGER.info(
            "%s runtime started with CPU=%s RAM=%s",
            self._prefix(),
            self.total_cpu,
            self.total_ram,
        )

        threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"worker-heartbeat-{self.worker_id[-6:]}",
        ).start()

        while not self._stop_event.is_set():
            try:
                available_cpu, available_ram = self._remaining_capacity()
                can_pull = available_cpu >= 1 and available_ram >= 1

                print(
                    f"[Worker {self.worker_id}] polling | CPU={available_cpu} RAM={available_ram}"
                )

                if can_pull:
                    self._poll_backoff = max(
                        self._initial_backoff,
                        self._poll_backoff * 0.5,
                    )
                self._had_capacity = can_pull

                if not can_pull:
                    self._poll_backoff = self._initial_backoff
                    self._bounded_poll_sleep()
                    continue

                response = self._request_with_retry(
                    "GET",
                    f"{TASK_URL}/get_task",
                    params={"worker_id": self.worker_id},
                    attempts=2,
                    timeout=1.5,
                )

                if response is None:
                    self._on_no_task_found()
                    self._bounded_poll_sleep()
                    continue

                if response.status_code == 200:
                    try:
                        tasks = response.json()
                    except ValueError:
                        LOGGER.warning(
                            "%s invalid JSON from /tasks/get_task", self._prefix()
                        )
                        self._on_no_task_found()
                        self._bounded_poll_sleep()
                        continue

                    if not isinstance(tasks, list):
                        LOGGER.warning(
                            "%s /tasks/get_task returned non-list payload",
                            self._prefix(),
                        )
                        tasks = []

                    print(f"[Worker {self.worker_id}] tasks fetched: {len(tasks)}")

                    if not tasks:
                        self._on_no_task_found()
                        self._bounded_poll_sleep()
                        continue

                    self._on_task_found()
                    pending_tasks_high = len(tasks) >= 3
                    if pending_tasks_high:
                        self._poll_backoff = 0.2

                    started = 0
                    for task in tasks:
                        if self._try_start_task(task):
                            started += 1
                            task_id = str(task.get("id", ""))
                            print(f"[Worker {self.worker_id}] picked task {task_id}")

                    if started == 0:
                        self._on_no_task_found()
                        self._bounded_poll_sleep()
                        continue

                    self._bounded_poll_sleep()
                    continue

                if response.status_code in (404, 409):
                    self._on_no_task_found()
                    self._bounded_poll_sleep()
                    continue

                if response.status_code == 429:
                    self._on_no_task_found()
                    self._bounded_poll_sleep()
                    continue

                LOGGER.warning(
                    "%s /tasks/get_task returned HTTP %s",
                    self._prefix(),
                    response.status_code,
                )
                self._on_no_task_found()
                self._bounded_poll_sleep()

            except Exception as exc:
                LOGGER.exception("%s runtime loop error: %s", self._prefix(), exc)
                self._on_no_task_found()
                self._bounded_poll_sleep()


def run(worker_id: str | None = None):
    if worker_id:
        total_cpu, total_ram = load_existing_worker(worker_id)
        label = "Worker"
    else:
        worker_id, total_cpu, total_ram = register_worker(simulate=False)
        label = "Worker"

    runtime = WorkerRuntime(
        worker_id=worker_id,
        total_cpu=total_cpu,
        total_ram=total_ram,
        label=label,
    )
    runtime.run_forever()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run worker runtime")
    parser.add_argument(
        "--worker-id",
        dest="worker_id",
        default=None,
        help="Existing worker id to attach runtime to",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(worker_id=args.worker_id)
