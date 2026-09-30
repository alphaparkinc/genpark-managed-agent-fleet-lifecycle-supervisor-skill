"""Example usage for ManagedAgentFleetLifecycleSupervisor."""
import json
from client import ManagedAgentFleetLifecycleSupervisor

def main():
    print("=== Managed Agent Fleet Lifecycle Supervisor Demo ===")
    supervisor = ManagedAgentFleetLifecycleSupervisor(heartbeat_timeout_sec=15.0, max_consecutive_failures=2)

    # 1. Register fleet workers (Tencent WorkBuddy style worker agents)
    w1 = supervisor.register_worker("agent_ppt_master", "PRESENTATION_SPECIALIST", ["ppt_generation", "graphic_layout", "slide_notes"], max_concurrency=2)
    w2 = supervisor.register_worker("agent_finance_calc", "FINANCIAL_ANALYST", ["excel_formula", "financial_modeling", "tax_audit"], max_concurrency=2)
    print("Workers Registered:")
    print(f" - {w1['agent_id']} ({w1['role']}): {w1['capabilities']}")
    print(f" - {w2['agent_id']} ({w2['role']}): {w2['capabilities']}")

    # 2. Dispatch Task
    print("\n--- Dispatching Q3 Financial Modeling Task ---")
    t1 = supervisor.dispatch_task(
        task_id="task_audit_001",
        required_capabilities=["financial_modeling"],
        task_payload={"company": "Acme Corp", "quarter": "Q3"}
    )
    print(json.dumps(t1, indent=2))

    # 3. Simulate Failures & Circuit Breaker Trigger
    print("\n--- Simulating Consecutive Worker Errors to Trip Circuit Breaker ---")
    supervisor.handle_task_result("task_audit_001", success=False, error_message="Database lock timeout")
    
    # Second task failing
    t2 = supervisor.dispatch_task("task_audit_002", ["financial_modeling"], {})
    supervisor.handle_task_result("task_audit_002", success=False, error_message="Rate limit exceeded")

    telemetry = supervisor.get_fleet_telemetry()
    print("\nFleet Telemetry Snapshot:")
    print(json.dumps(telemetry, indent=2))

if __name__ == "__main__":
    main()
