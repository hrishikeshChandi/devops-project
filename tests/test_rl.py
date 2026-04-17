import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scheduling.rl_agent import QLearningAgent
from scheduling.estimator import DurationEstimator


class TestQLearningAgent:
    def test_discretize_state(self):
        agent = QLearningAgent()
        state = agent.discretize_state(10, 16, 4, 8)
        assert state == "3-2-1-2"

    def test_get_weights_default(self):
        agent = QLearningAgent()
        agent.q_table = {}
        w_prio, w_dur = agent.get_weights("non-existent")
        assert w_prio == 0.7
        assert w_dur == 0.3

    def test_update_q(self):
        agent = QLearningAgent()
        agent.q_table = {}
        state = "1-1-1-1"
        agent.update_q(state, 0, 10.0, state)
        assert state in agent.q_table
        assert agent.q_table[state][0] > 0


class TestDurationEstimator:
    def test_get_cpu_bin(self):
        assert DurationEstimator.get_cpu_bin(1) == "0-2"
        assert DurationEstimator.get_cpu_bin(3) == "2-4"
        assert DurationEstimator.get_cpu_bin(6) == "4-8"
        assert DurationEstimator.get_cpu_bin(12) == "8-16"
        assert DurationEstimator.get_cpu_bin(20) == "16+"

    def test_get_ram_bin(self):
        assert DurationEstimator.get_ram_bin(2) == "0-4"
        assert DurationEstimator.get_ram_bin(6) == "4-8"
        assert DurationEstimator.get_ram_bin(12) == "8-16"
        assert DurationEstimator.get_ram_bin(24) == "16-32"
        assert DurationEstimator.get_ram_bin(40) == "32+"
