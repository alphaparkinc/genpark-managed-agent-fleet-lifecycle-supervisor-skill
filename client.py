"""
Enterprise Managed Agent Fleet Lifecycle Supervisor (Zero External Dependencies)
Provides capability matching, heartbeat monitoring, circuit breaker isolation, and fleet telemetry.
"""
import time
import json
from typing import Dict, Any, List, Optional

class ManagedAgentFleetLifecycleSupervisor:
    def __init__(self, heartbeat_timeout_sec: float = 30.0, max_consecutive_failures: int = 3):
        self.heartbeat_timeout = heartbeat_timeout_sec
        self.max_failures = max_consecutive_failures
        self.workers: Dict[str, Dict[str, Any]] = {}
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.task_history: List[Dict[str, Any]] = []

    def register_worker(
        self,
        agent_id: str,
        role: str = "GENERALIST",
        capabilities: Optional[List[str]] = None,
        max_concurrency: int = 2,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Registers a worker agent in the enterprise fleet registry."""
        capabilities = capabilities or ["general_reasoning"]
        now = time.time()
        self.workers[agent_id] = {
            "agent_id": agent_id,
            "role": role,
            "capabilities": set(capabilities),
            "max_concurrency": max_concurrency,
            "status": "IDLE", # IDLE, BUSY, DEGRADED, DRAINED, DEAD
            "active_tasks": [],
            "consecutive_failures": 0,
            "total_tasks_completed": 0,
            "total_tasks_failed": 0,
            "circuit_breaker": "CLOSED", # CLOSED, OPEN, HALF_OPEN
            "last_heartbeat": now,
            "registered_at": now,
            "metadata": metadata or {}
        }
        return {
            "registered": True,
            "agent_id": agent_id,
            "role": role,
            "status": "IDLE",
            "capabilities": list(self.workers[agent_id]["capabilities"])
        }

    def record_heartbeat(
        self,
        agent_id: str,
        status: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Records agent liveness and performance telemetry."""
        if agent_id not in self.workers:
            return {"error": f"Agent {agent_id} not registered"}

        w = self.workers[agent_id]
        w["last_heartbeat"] = time.time()
        if status and w["circuit_breaker"] != "OPEN":
            w["status"] = status

        if metrics:
            w["last_metrics"] = metrics

        return {
            "agent_id": agent_id,
            "status": w["status"],
            "circuit_breaker": w["circuit_breaker"],
            "active_tasks_count": len(w["active_tasks"]),
            "timestamp": w["last_heartbeat"]
        }

    def dispatch_task(
        self,
        task_id: str,
        required_capabilities: List[str],
        task_payload: Optional[Dict[str, Any]] = None,
        priority: int = 1
    ) -> Dict[str, Any]:
        """
        Dispatches task to the most suitable healthy worker based on:
        1. Capability coverage match
        2. Worker state (not DEAD/DRAINED/OPEN)
        3. Concurrency capacity
        4. Historical reliability score
        """
        now = time.time()
        # Clean up stale heartbeats first
        for wid, w in self.workers.items():
            if now - w["last_heartbeat"] > self.heartbeat_timeout and w["status"] != "DEAD":
                w["status"] = "DEAD"

        req_set = set(required_capabilities)
        candidates = []

        for wid, w in self.workers.items():
            if w["status"] in ("DEAD", "DRAINED") or w["circuit_breaker"] == "OPEN":
                continue
            if len(w["active_tasks"]) >= w["max_concurrency"]:
                continue

            # Check capability intersection
            overlap = len(req_set.intersection(w["capabilities"]))
            match_ratio = overlap / max(1, len(req_set))
            if match_ratio < 0.5 and len(req_set) > 0:
                continue

            # Reliability score
            total = w["total_tasks_completed"] + w["total_tasks_failed"]
            success_rate = (w["total_tasks_completed"] / total) if total > 0 else 1.0
            load_factor = 1.0 - (len(w["active_tasks"]) / w["max_concurrency"])

            composite_score = 0.5 * match_ratio + 0.3 * success_rate + 0.2 * load_factor
            candidates.append({"worker": w, "score": composite_score})

        if not candidates:
            return {
                "dispatched": False,
                "task_id": task_id,
                "reason": "No healthy workers matching capability/concurrency constraints"
            }

        candidates.sort(key=lambda x: x["score"], reverse=True)
        chosen = candidates[0]["worker"]

        # Assign task
        chosen["active_tasks"].append(task_id)
        if len(chosen["active_tasks"]) >= chosen["max_concurrency"]:
            chosen["status"] = "BUSY"

        self.tasks[task_id] = {
            "task_id": task_id,
            "assigned_agent_id": chosen["agent_id"],
            "required_capabilities": required_capabilities,
            "dispatched_at": now,
            "status": "RUNNING",
            "priority": priority,
            "payload": task_payload or {}
        }

        return {
            "dispatched": True,
            "task_id": task_id,
            "assigned_agent_id": chosen["agent_id"],
            "worker_role": chosen["role"],
            "fit_score": round(candidates[0]["score"], 4)
        }

    def handle_task_result(
        self,
        task_id: str,
        success: bool,
        error_message: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Processes task completion, updates success counters, or trips circuit breaker."""
        if task_id not in self.tasks:
            return {"error": f"Unknown task {task_id}"}

        task = self.tasks[task_id]
        wid = task["assigned_agent_id"]
        w = self.workers.get(wid)

        task["status"] = "COMPLETED" if success else "FAILED"
        task["completed_at"] = time.time()
        task["duration_sec"] = round(task["completed_at"] - task["dispatched_at"], 3)
        task["error"] = error_message
        task["result"] = result_data

        if w:
            if task_id in w["active_tasks"]:
                w["active_tasks"].remove(task_id)
            if success:
                w["total_tasks_completed"] += 1
                w["consecutive_failures"] = 0
                if w["circuit_breaker"] == "HALF_OPEN":
                    w["circuit_breaker"] = "CLOSED"
                if len(w["active_tasks"]) == 0 and w["status"] != "DRAINED":
                    w["status"] = "IDLE"
            else:
                w["total_tasks_failed"] += 1
                w["consecutive_failures"] += 1
                if w["consecutive_failures"] >= self.max_failures:
                    w["circuit_breaker"] = "OPEN"
                    w["status"] = "DEGRADED"

        self.task_history.append(task)
        return {
            "task_id": task_id,
            "agent_id": wid,
            "status": task["status"],
            "circuit_breaker_status": w["circuit_breaker"] if w else "UNKNOWN"
        }

    def trip_circuit_breaker(self, agent_id: str, reason: str = "Manual override") -> Dict[str, Any]:
        """Manually trips circuit breaker for an agent to isolate it from incoming requests."""
        if agent_id not in self.workers:
            return {"error": f"Agent {agent_id} not found"}
        w = self.workers[agent_id]
        w["circuit_breaker"] = "OPEN"
        w["status"] = "DEGRADED"
        return {"agent_id": agent_id, "circuit_breaker": "OPEN", "reason": reason}

    def drain_worker(self, agent_id: str) -> Dict[str, Any]:
        """Sets worker into DRAIN mode so it finishes existing tasks but takes no new ones."""
        if agent_id not in self.workers:
            return {"error": f"Agent {agent_id} not found"}
        w = self.workers[agent_id]
        w["status"] = "DRAINED"
        return {"agent_id": agent_id, "status": "DRAINED", "pending_tasks": len(w["active_tasks"])}

    def get_fleet_telemetry(self) -> Dict[str, Any]:
        """Returns comprehensive fleet health, utilization, and error metrics."""
        now = time.time()
        counts = {"IDLE": 0, "BUSY": 0, "DEGRADED": 0, "DRAINED": 0, "DEAD": 0}
        breakers = {"CLOSED": 0, "OPEN": 0, "HALF_OPEN": 0}

        worker_summaries = []
        for wid, w in self.workers.items():
            if now - w["last_heartbeat"] > self.heartbeat_timeout and w["status"] != "DEAD":
                w["status"] = "DEAD"

            counts[w["status"]] = counts.get(w["status"], 0) + 1
            breakers[w["circuit_breaker"]] = breakers.get(w["circuit_breaker"], 0) + 1

            worker_summaries.append({
                "agent_id": wid,
                "role": w["role"],
                "status": w["status"],
                "circuit_breaker": w["circuit_breaker"],
                "active_tasks": len(w["active_tasks"]),
                "completed": w["total_tasks_completed"],
                "failed": w["total_tasks_failed"],
                "consecutive_failures": w["consecutive_failures"],
                "heartbeat_age_sec": round(now - w["last_heartbeat"], 1)
            })

        return {
            "total_workers": len(self.workers),
            "status_distribution": counts,
            "circuit_breaker_distribution": breakers,
            "total_historical_tasks": len(self.task_history),
            "workers": worker_summaries
        }
