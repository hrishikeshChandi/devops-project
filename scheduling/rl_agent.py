import logging
import random
import threading

import numpy as np
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

from config.constants import RL_ALPHA, RL_EPSILON, RL_GAMMA
from db.connection import q_table_collection, run_with_mongo_retry

LOGGER = logging.getLogger("rl-agent")


class QLearningAgent:
    def __init__(self):
        self.q_table: dict[str, list[float]] = {}
        self.epsilon = RL_EPSILON
        self._loaded = False
        self._load_lock = threading.Lock()

    def _load_q_table(self) -> dict[str, list[float]]:
        def _read_q_table() -> dict[str, list[float]]:
            loaded: dict[str, list[float]] = {}
            for doc in q_table_collection.find({}, {"state": 1, "values": 1}):
                state = doc.get("state")
                values = doc.get("values")
                if (
                    isinstance(state, str)
                    and isinstance(values, list)
                    and len(values) >= 2
                ):
                    loaded[state] = [float(values[0]), float(values[1])]
            return loaded

        try:
            loaded = run_with_mongo_retry(
                _read_q_table,
                operation_name="q_table_load",
                max_attempts=4,
            )
            LOGGER.info("Q-table loaded with %s states", len(loaded))
            return loaded
        except PyMongoError as exc:
            LOGGER.warning(
                "Unable to load Q-table from MongoDB, using empty in-memory table: %s",
                exc,
            )
            return {}

    def _ensure_loaded(self):
        if self._loaded:
            return

        with self._load_lock:
            if self._loaded:
                return
            self.q_table = self._load_q_table()
            self._loaded = True

    def save_q_table(self) -> bool:
        self._ensure_loaded()

        if not self.q_table:
            return True

        operations = [
            UpdateOne(
                {"state": state},
                {"$set": {"values": [float(values[0]), float(values[1])]}},
                upsert=True,
            )
            for state, values in self.q_table.items()
            if len(values) >= 2
        ]

        if not operations:
            return True

        def _write_q_table() -> None:
            q_table_collection.bulk_write(operations, ordered=False)

        try:
            run_with_mongo_retry(
                _write_q_table,
                operation_name="q_table_save",
                max_attempts=4,
            )
            return True
        except PyMongoError as exc:
            LOGGER.warning("Unable to persist Q-table, continuing in-memory: %s", exc)
            return False

    def _get_cpu_bin(self, cpu: int) -> int:
        """Discretize CPU into bins: 0-2, 2-4, 4-8, 8-16, 16+"""
        if cpu <= 2:
            return 0
        if cpu <= 4:
            return 1
        if cpu <= 8:
            return 2
        if cpu <= 16:
            return 3
        return 4

    def _get_ram_bin(self, ram: int) -> int:
        """Discretize RAM into bins: 0-4, 4-8, 8-16, 16-32, 32+"""
        if ram <= 4:
            return 0
        if ram <= 8:
            return 1
        if ram <= 16:
            return 2
        if ram <= 32:
            return 3
        return 4

    def discretize_state(
        self, worker_cpu: int, worker_ram: int, task_cpu: int, task_ram: int
    ) -> str:
        """Create a hashable state string from binned values."""
        w_cpu_bin = self._get_cpu_bin(worker_cpu)
        w_ram_bin = self._get_ram_bin(worker_ram)
        t_cpu_bin = self._get_cpu_bin(task_cpu)
        t_ram_bin = self._get_ram_bin(task_ram)
        return f"{w_cpu_bin}-{w_ram_bin}-{t_cpu_bin}-{t_ram_bin}"

    def decay_epsilon(self):
        """Decay epsilon for less exploration over time."""
        from config.constants import RL_EPSILON_DECAY

        self.epsilon = max(0.05, self.epsilon * RL_EPSILON_DECAY)

    def get_action(self, state: str, force_explore: bool = False) -> int:
        """Return 0 (favour priority) or 1 (favour duration)."""
        self._ensure_loaded()

        if state not in self.q_table:
            self.q_table[state] = [0.0, 0.0]

        if force_explore or random.random() < self.epsilon:
            return random.randint(0, 1)

        return int(np.argmax(self.q_table[state]))

    def get_weights(self, state: str) -> tuple:
        """
        Return (w_priority, w_duration) derived from Q-values via softmax.
        Default to (0.7, 0.3) if no entry.
        """
        self._ensure_loaded()

        if state not in self.q_table:
            return 0.7, 0.3

        q_values = self.q_table[state]

        # Subtract max for numerical stability.
        max_q = max(q_values[0], q_values[1])
        exp_q = np.exp([q_values[0] - max_q, q_values[1] - max_q])
        total = np.sum(exp_q)

        if total > 0:
            w_priority = exp_q[0] / total
            w_duration = exp_q[1] / total
        else:
            w_priority, w_duration = 0.7, 0.3

        return w_priority, w_duration

    def update_q(self, state: str, action: int, reward: float, next_state: str):
        """Standard Q-learning update rule."""
        self._ensure_loaded()

        if state not in self.q_table:
            self.q_table[state] = [0.0, 0.0]
        if next_state not in self.q_table:
            self.q_table[next_state] = [0.0, 0.0]

        best_next_q = max(self.q_table[next_state])
        current_q = self.q_table[state][action]

        new_q = current_q + RL_ALPHA * (reward + RL_GAMMA * best_next_q - current_q)
        self.q_table[state][action] = new_q
