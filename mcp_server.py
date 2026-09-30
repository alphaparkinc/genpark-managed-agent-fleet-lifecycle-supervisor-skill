"""MCP Server for Managed Agent Fleet Lifecycle Supervisor."""
import sys
import json
import time
from client import ManagedAgentFleetLifecycleSupervisor

supervisor = ManagedAgentFleetLifecycleSupervisor()

def handle_call_tool(params):
    name = params.get("name")
    args = params.get("arguments", {})
    if name != "manage_agent_fleet":
        raise ValueError(f"Unknown tool: {name}")

    action = args.get("action", "get_fleet_telemetry")
    if action == "register_worker":
        return supervisor.register_worker(
            agent_id=args.get("agent_id", "worker_1"),
            role=args.get("role", "GENERALIST"),
            capabilities=args.get("capabilities"),
            max_concurrency=int(args.get("max_concurrency", 2))
        )
    elif action == "dispatch_task":
        return supervisor.dispatch_task(
            task_id=args.get("task_id", f"task_{time.time()}"),
            required_capabilities=args.get("required_capabilities", []),
            task_payload=args.get("task_payload")
        )
    elif action == "record_heartbeat":
        return supervisor.record_heartbeat(
            agent_id=args.get("agent_id"),
            status=args.get("status")
        )
    elif action == "handle_task_result":
        return supervisor.handle_task_result(
            task_id=args.get("task_id"),
            success=bool(args.get("success", True)),
            error_message=args.get("error_message")
        )
    elif action == "trip_circuit_breaker":
        return supervisor.trip_circuit_breaker(args.get("agent_id"))
    elif action == "drain_worker":
        return supervisor.drain_worker(args.get("agent_id"))
    elif action == "get_fleet_telemetry":
        return supervisor.get_fleet_telemetry()
    else:
        raise ValueError(f"Invalid action: {action}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Running self-test...")
        reg = supervisor.register_worker("test_w1", "DATA_ANALYST", ["sql", "excel"], max_concurrency=2)
        assert reg["registered"] is True
        disp = supervisor.dispatch_task("t1", ["sql"], {"query": "SELECT count(*) FROM users"})
        assert disp["dispatched"] is True
        res = supervisor.handle_task_result("t1", True)
        assert res["status"] == "COMPLETED"
        telem = supervisor.get_fleet_telemetry()
        assert telem["total_workers"] >= 1
        print("Self-test PASSED!")
        sys.exit(0)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            msg_id = req.get("id")
            method = req.get("method")
            if method == "initialize":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "serverInfo": {"name": "ManagedAgentFleetLifecycleSupervisor", "version": "1.0.0"},
                        "capabilities": {"tools": {}}
                    }
                }
            elif method == "tools/list":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "tools": [{
                            "name": "manage_agent_fleet",
                            "description": "Enterprise agent fleet supervision: register workers, dispatch tasks with capability routing, monitor heartbeat telemetry, trip circuit breakers, and retrieve cluster health.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "action": {"type": "string", "enum": ["register_worker", "dispatch_task", "record_heartbeat", "handle_task_result", "trip_circuit_breaker", "get_fleet_telemetry", "drain_worker"]},
                                    "agent_id": {"type": "string"},
                                    "role": {"type": "string"},
                                    "capabilities": {"type": "array"},
                                    "max_concurrency": {"type": "integer"},
                                    "task_id": {"type": "string"},
                                    "required_capabilities": {"type": "array"},
                                    "task_payload": {"type": "object"},
                                    "status": {"type": "string"},
                                    "success": {"type": "boolean"},
                                    "error_message": {"type": "string"}
                                },
                                "required": ["action"]
                            }
                        }]
                    }
                }
            elif method == "tools/call":
                res = handle_call_tool(req.get("params", {}))
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(res, indent=2)}]}
                }
            else:
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
            print(json.dumps(resp), flush=True)
        except Exception as e:
            err_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": str(e)}}
            print(json.dumps(err_resp), flush=True)

if __name__ == "__main__":
    main()
