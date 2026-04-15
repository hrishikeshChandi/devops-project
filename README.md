# 🚀 Distributed Task Orchestrator

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green.svg)](https://www.mongodb.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A **distributed task orchestrator** with resource-aware scheduling, fault tolerance, and real-time monitoring. Built with FastAPI and MongoDB.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [How It Works](#how-it-works)
- [Diagrams](#diagrams)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the System](#running-the-system)
- [API Endpoints](#api-endpoints)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Overview

The **Distributed Task Orchestrator** is a system that intelligently distributes computational tasks across a fleet of worker nodes. Workers register with their hardware specifications (CPU cores, RAM), and tasks specify resource requirements. The system matches tasks to capable workers, tracks resource allocation, and automatically recovers from failures.

**Real-world analogy:** Like Kubernetes for task scheduling, but simpler and purpose-built for understanding distributed systems concepts.

---

## Features

### Core Features

| Feature                       | Description                                              |
| ----------------------------- | -------------------------------------------------------- |
| **Worker Registration**       | Workers self-register with random CPU/RAM specs          |
| **Resource-Aware Scheduling** | Tasks assigned only to workers with sufficient resources |
| **Task Lifecycle Management** | Complete tracking: pending → running → completed/failed  |
| **Priority Queue**            | Higher priority tasks processed first (integer priority) |
| **Rate Limiting**             | Protection against excessive API requests using SlowAPI  |

### Fault Tolerance

| Feature                   | Description                                                       |
| ------------------------- | ----------------------------------------------------------------- |
| **Heartbeat Monitoring**  | Workers send heartbeats every 5 seconds                           |
| **Dead Worker Detection** | Workers missing heartbeat for 30 seconds are marked dead          |
| **Task Recovery**         | Stuck tasks (>30s) are reset and retried (max 3 attempts)         |
| **Resource Cleanup**      | Resources freed on completion, failure, timeout, or worker death  |
| **Atomic Claiming**       | Atomic MongoDB updates prevent race conditions in task assignment |

---

## System Architecture

The system consists of:

- **API Server** (FastAPI) — Handles task submission and worker communication
- **MongoDB** (Standalone) — Stores tasks and worker state using atomic updates
- **Worker Nodes** — Execute tasks, each with independent heartbeat thread
- **Background Services** — Recovery daemon and heartbeat monitor for fault tolerance

**[View System Architecture Diagram](diagrams/system-architecture.png)**

---

## How It Works

### Task Lifecycle

Tasks progress through four distinct states:

- **Submit** — Client submits task with CPU/RAM requirements and optional priority value
- **Pending** — Task enters queue, waiting for a worker with sufficient available resources
- **Running** — Worker claims task; system atomically allocates resources and locks them
- **Completed/Failed** — Worker reports final status; system releases allocated resources

### Task Assignment

Workers pull tasks rather than being pushed, using a race-free atomic process:

1. Worker requests a task via `GET /tasks/get_task` with its worker ID
2. System finds highest priority pending task matching worker's available CPU and RAM
3. MongoDB `find_one_and_update` atomically claims one task and marks it running
4. Worker resources are reserved with conditional `$inc`; if reservation fails, task claim is reverted and retried
5. Successful allocation returns task to worker with allocated resources recorded

### Resource Management

- Each worker tracks total capacity and currently available resources
- Tasks specify required CPU cores and RAM in MB
- Resources are allocated at task start and freed upon completion or failure
- Dead workers have their orphaned tasks recovered and resources released

### Fault Tolerance

- **Heartbeat system** — Workers send heartbeats every 5 seconds; missing 3 consecutive heartbeats marks worker as dead
- **Task timeout** — Tasks running longer than 30 seconds are automatically reset and retried
- **Retry mechanism** — Failed tasks retry up to 3 times before being marked as permanently failed
- **Resource cleanup** — Background processes automatically free resources from dead workers and timed-out tasks

### Priority System

- Tasks include an integer priority field (higher numbers = higher priority)
- When multiple tasks are pending, the system selects the highest priority task
- For tasks with equal priority, older tasks (by creation time) are selected first
- Priority ensures critical tasks skip the queue without starving lower priority tasks

---

## Diagrams

| Diagram                                                               | Description                                               |
| --------------------------------------------------------------------- | --------------------------------------------------------- |
| [System Architecture](diagrams/system-architecture.png)               | Overall system components and their interactions          |
| [Task Lifecycle](diagrams/task-lifecycle.png)                         | State transitions for tasks from submission to completion |
| [Task Assignment Flow](diagrams/task-assignment-flow.png)             | Flowchart of how tasks are assigned to workers            |
| [Resource Allocation Flow](diagrams/resource-allocation-flow.png)     | How resources are allocated and tracked                   |
| [Worker Lifecycle](diagrams/worker-lifecycle.png)                     | Worker states and transitions                             |
| [Fault Tolerance Mechanisms](diagrams/fault-tolerance-mechanisms.png) | Recovery processes for failures                           |
| [Database Schema](diagrams/database-schema.png)                       | MongoDB collections and document structures               |
| [Priority Ordering](diagrams/priority-ordering.png)                   | How tasks are prioritized in the queue                    |

---

## Tech Stack

| Component                | Technology           |
| ------------------------ | -------------------- |
| **Backend Framework**    | FastAPI              |
| **Database**             | MongoDB (Standalone) |
| **Worker Communication** | HTTP REST APIs       |
| **Rate Limiting**        | SlowAPI              |
| **Data Validation**      | Pydantic v2          |
| **Python Version**       | 3.12+                |

---

## Prerequisites

- **Python 3.12+** — [Download](https://www.python.org/downloads/)
- **MongoDB 7.0+** — [Download](https://www.mongodb.com/try/download/community)
- **uv** — [Download](https://docs.astral.sh/uv/getting-started/installation/)
- **Git** — [Download](https://git-scm.com/)

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/hrishikeshChandi/distributed-task-orchestrator
cd distributed-task-orchestrator
```

### 2. Create Virtual Environment and install the dependencies

```bash
uv sync
source .venv/bin/activate # Linux/macOS

# .venv\Scripts\activate # Windows
```

### 3. Start MongoDB

```bash
# Start standalone MongoDB
docker compose up -d mongo
```

### 4. Configure Environment

Create `.env` file in `config/` directory:

```env
HOST=localhost
MODULE=main:app
PORT=8000
MONGO_URI=mongodb://localhost:27017
```

---

## Configuration

| Variable             | Default                     | Description                             |
| -------------------- | --------------------------- | --------------------------------------- |
| `HOST`               | `localhost`                 | Server host                             |
| `PORT`               | `8000`                      | Server port                             |
| `MONGO_URI`          | `mongodb://localhost:27017` | MongoDB connection string               |
| `TIMEOUT`            | `30`                        | Task timeout in seconds                 |
| `MAX_RETRIES`        | `3`                         | Maximum retry attempts for failed tasks |
| `HEARTBEAT_INTERVAL` | `5`                         | Worker heartbeat interval in seconds    |
| `HEARTBEAT_TIMEOUT`  | `30`                        | Worker heartbeat timeout in seconds     |

---

## Running the System

### Terminal 1: MongoDB

```bash
docker compose up -d mongo
```

### Terminal 2: FastAPI Server

```bash
uv run main.py
```

### Terminal 3: Recovery Daemon

```bash
uv run -m recovery.recovery
```

### Terminal 4: Heartbeat Monitor

```bash
uv run -m workers.heartbeat
```

### Terminal 5-7: Workers (Run multiple)

```bash
uv run -m workers.worker  # First worker
uv run -m workers.worker  # Second worker
uv run -m workers.worker  # Third worker
```

---

## API Endpoints

### Tasks

| Method | Endpoint                         | Description            | Rate Limit |
| ------ | -------------------------------- | ---------------------- | ---------- |
| `POST` | `/tasks/submit`                  | Submit a new task      | 20/min     |
| `GET`  | `/tasks/get_task`                | Worker requests a task | 20/sec     |
| `GET`  | `/tasks/tasks`                   | List pending tasks     | Unlimited  |
| `PUT`  | `/tasks/update_status/{task_id}` | Update task status     | 20/min     |

### Workers

| Method | Endpoint                         | Description             | Rate Limit |
| ------ | -------------------------------- | ----------------------- | ---------- |
| `POST` | `/workers/add_worker`            | Register a new worker   | 20/min     |
| `PUT`  | `/workers/heartbeat/{worker_id}` | Update worker heartbeat | 20/min     |
| `GET`  | `/workers/get_workers`           | List all workers        | 10/min     |

---

## Testing

### Submit a Task

```bash
curl -X POST http://localhost:8000/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{"data": {"task": "test"}, "required_cpu": 2, "required_ram": 4}'
```

### Submit High Priority Task

```bash
curl -X POST http://localhost:8000/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{"data": {"task": "urgent"}, "required_cpu": 2, "required_ram": 4, "priority": 5}'
```

### Check Workers

```bash
curl http://localhost:8000/workers/get_workers
```

### Check Pending Tasks

```bash
curl http://localhost:8000/tasks/tasks
```

### Submit Multiple Tasks (Bash Loop)

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8000/tasks/submit \
    -H "Content-Type: application/json" \
    -d "{\"data\": {\"task\": \"test_$i\"}, \"required_cpu\": 2, \"required_ram\": 4}"
done
```

---

## Project Structure

```
orchestrator/
├── config/
│   ├── __init__.py
│   ├── constants.py          # Configuration constants
│   └── .env                  # Environment variables
├── core/
│   ├── __init__.py
│   └── limiter.py            # Rate limiting setup
├── db/
│   ├── __init__.py
│   └── connection.py         # MongoDB connection
├── models/
│   ├── __init__.py
│   └── model.py              # Pydantic models
├── routers/
│   ├── __init__.py
│   ├── task.py               # Task endpoints
│   └── workers.py            # Worker endpoints
├── workers/
│   ├── __init__.py
│   ├── worker.py             # Worker process
│   └── heartbeat.py          # Heartbeat monitor
├── recovery/
│   ├── __init__.py
│   └── recovery.py           # Recovery daemon
├── main.py                   # FastAPI entry point
└── README.md                 # This file
```

---

## Troubleshooting

### MongoDB Connection Error

```
ServerSelectionTimeoutError
```

**Solution:** Ensure standalone MongoDB is running:

```bash
docker compose up -d mongo
docker compose exec mongo mongosh --eval "db.adminCommand('ping')"
```

### Rate Limit Hit

```
429 Too Many Requests
```

**Solution:** Wait a few seconds or adjust limits in `routers/task.py` and `routers/workers.py`

### Worker Not Getting Tasks

- Check if workers are registered: `curl http://localhost:8000/workers/get_workers`
- Check if any tasks match worker resources
- Verify heartbeat monitor and recovery daemon are running
- Check MongoDB connectivity and task/worker indexes

### Task Stuck in Running State

- Verify recovery daemon is running
- Check task timeout configuration in `config/constants.py`
- Monitor logs for recovery daemon activity

---

## License

This project is licensed under the MIT License.
