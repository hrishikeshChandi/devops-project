import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST", "127.0.0.1")
MODULE = os.getenv("MODULE", "main:app")
PORT = int(os.getenv("PORT", "8000"))
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "orchestrator")
MONGO_RETRY_BASE_SECONDS = float(os.getenv("MONGO_RETRY_BASE_SECONDS", "0.5"))
MONGO_RETRY_MAX_SECONDS = float(os.getenv("MONGO_RETRY_MAX_SECONDS", "5.0"))

TIMEOUT = 30
MAX_RETRIES = 3

# ✅ NEW
HEARTBEAT_INTERVAL = 5
HEARTBEAT_TIMEOUT = 30

# RL Scheduler
RL_ENABLED = True
RL_EPSILON = 0.2
RL_EPSILON_DECAY = 0.995
RL_ALPHA = 0.1
RL_GAMMA = 0.9
RL_UPDATE_INTERVAL = 30
