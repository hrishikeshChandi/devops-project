from scheduling.estimator import DurationEstimator
from scheduling.rl_agent import QLearningAgent


class TestQLearningAgent:
    def test_discretize_state(self):
        agent = QLearningAgent()
        state = agent.discretize_state(10, 16, 4, 8)
        assert state == "3-2-1-2"

    def test_get_weights_default(self):
        agent = QLearningAgent()
        agent.q_table = {}
        w1, w2 = agent.get_weights("x")
        assert w1 == 0.7 and w2 == 0.3

    def test_update_q(self):
        agent = QLearningAgent()
        agent.q_table = {}
        agent.update_q("s", 0, 10, "s")
        assert "s" in agent.q_table


class TestDurationEstimator:
    def test_cpu_bin(self):
        assert DurationEstimator.get_cpu_bin(1) == "0-2"
        assert DurationEstimator.get_cpu_bin(20) == "16+"

    def test_ram_bin(self):
        assert DurationEstimator.get_ram_bin(2) == "0-4"
        assert DurationEstimator.get_ram_bin(40) == "32+"
