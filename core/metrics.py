from prometheus_client import Counter, Gauge

tasks_submitted_total = Counter(
    "tasks_submitted_total",
    "Total number of tasks submitted",
)

tasks_completed_total = Counter(
    "tasks_completed_total",
    "Total number of tasks completed successfully",
)

tasks_failed_total = Counter(
    "tasks_failed_total",
    "Total number of tasks failed",
)

pending_tasks_gauge = Gauge(
    "pending_tasks",
    "Current number of pending tasks",
)

active_workers_gauge = Gauge(
    "active_workers",
    "Current number of active workers",
)

rl_success_rate_gauge = Gauge(
    "rl_success_rate",
    "RL success rate",
)
