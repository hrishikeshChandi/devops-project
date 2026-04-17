import logging
import threading

from pymongo.errors import PyMongoError

from config.constants import RL_ENABLED, RL_UPDATE_INTERVAL
from db.connection import run_with_mongo_retry, task_history_collection
from scheduling.rl_agent import QLearningAgent

LOGGER = logging.getLogger("rl-learner")

_LEARNER_LOCK = threading.Lock()
_LEARNER_INSTANCE = None


class RLBackgroundLearner:
    def __init__(self):
        self.agent = QLearningAgent()
        self.stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def compute_reward(self, scheduling_valid: bool) -> float:
        # Learn only scheduling correctness, never execution randomness.
        return 1.0 if scheduling_valid else -1.0

    def run(self):
        transient_backoff = 1.0

        while not self.stop_event.is_set():
            if not RL_ENABLED:
                self.stop_event.wait(RL_UPDATE_INTERVAL)
                continue

            try:
                unprocessed_tasks = run_with_mongo_retry(
                    lambda: list(
                        task_history_collection.find(
                            {"processed_by_learner": {"$ne": True}}
                        ).limit(500)
                    ),
                    operation_name="learner_fetch_unprocessed",
                    max_attempts=3,
                )
                transient_backoff = 1.0
            except PyMongoError as exc:
                LOGGER.warning(
                    "RL learner could not fetch history from MongoDB: %s", exc
                )
                self.stop_event.wait(min(15.0, transient_backoff))
                transient_backoff = min(15.0, transient_backoff * 2)
                continue

            if unprocessed_tasks:
                updates_made = False
                for task_hist in unprocessed_tasks:
                    state = task_hist.get("rl_state")
                    action = task_hist.get("rl_action_taken")
                    scheduling_valid = bool(task_hist.get("scheduling_valid", True))

                    if state and action is not None:
                        reward = self.compute_reward(scheduling_valid)
                        self.agent.update_q(state, action, reward, state)
                        updates_made = True

                    try:
                        run_with_mongo_retry(
                            lambda task_hist=task_hist: task_history_collection.update_one(
                                {"_id": task_hist["_id"]},
                                {"$set": {"processed_by_learner": True}},
                            ),
                            operation_name="learner_mark_processed",
                            max_attempts=3,
                        )
                    except PyMongoError as exc:
                        LOGGER.warning(
                            "RL learner could not mark history as processed (%s): %s",
                            task_hist.get("_id"),
                            exc,
                        )

                if updates_made:
                    self.agent.save_q_table()
                    self.agent.decay_epsilon()
                    LOGGER.info(
                        "RL learner updated Q-table with %s tasks; epsilon=%.3f",
                        len(unprocessed_tasks),
                        self.agent.epsilon,
                    )

            self.stop_event.wait(RL_UPDATE_INTERVAL)


def start_learner():
    global _LEARNER_INSTANCE

    with _LEARNER_LOCK:
        if _LEARNER_INSTANCE is not None:
            return _LEARNER_INSTANCE

        learner = RLBackgroundLearner()
        thread = threading.Thread(target=learner.run, daemon=True, name="rl-learner")
        learner._thread = thread
        thread.start()

        _LEARNER_INSTANCE = learner
        return learner
