import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import TypedDict, Optional

from langgraph.graph import StateGraph

from flapping_agent import flapping_agent
from topology_agent import topology_agent
from communication_agent import communication_agent

# -----------------------------
# GLOBAL STORAGE
# -----------------------------

decisions = []
communication_logs = []
mtta_records = []
mttr_records = []

# -----------------------------
# SLA CONFIG
# -----------------------------

SLA_CONFIG = {
    "P1": {"mtta": 30, "mttr": 300},
    "P2": {"mtta": 60, "mttr": 600},
    "P3": {"mtta": 120, "mttr": 1200}
}

# -----------------------------
# STATE
# -----------------------------

class AlarmState(TypedDict):
    alarm: dict
    is_flapping: bool
    topology_suppressed: bool
    root_device: Optional[str]
    root_type: Optional[str]
    impact_radius: int
    decision: str

# -----------------------------
# UTILS
# -----------------------------

def get_ts(alarm):
    return datetime.fromisoformat(alarm["createdAt"].replace("Z",""))

# -----------------------------
# PRIORITY
# -----------------------------

def get_priority(alarm, impact):

    severity = alarm.get("severity", "")

    if isinstance(severity, dict):
        severity = severity.get("adjusted", "")

    severity = severity.lower()

    if severity == "critical" and impact > 200:
        return "P1"
    elif severity == "critical":
        return "P2"
    elif severity == "major":
        return "P2"
    else:
        return "P3"

# -----------------------------
# ROOT MULTIPLIER
# -----------------------------

def get_root_multiplier(root_type):

    if root_type == "CORE":
        return 2.0
    elif root_type == "AGG":
        return 1.5
    else:
        return 1.0

# -----------------------------
# LIFECYCLE
# -----------------------------

def inject_lifecycle(alarm, state):

    ts_open = get_ts(alarm)

    impact = state.get("impact_radius", 0)
    root_type = state.get("root_type")

    priority = get_priority(alarm, impact)
    sla = SLA_CONFIG[priority]

    multiplier = get_root_multiplier(root_type)

    mtta = sla["mtta"]
    mttr = int(sla["mttr"] * multiplier)

    ack_time = ts_open + timedelta(seconds=mtta)
    clear_time = ts_open + timedelta(seconds=mttr)

    return ack_time, clear_time, priority

# -----------------------------
# METRICS
# -----------------------------

def update_metrics(alarm, state):

    alarm_id = alarm["alarm_id"]
    ts_open = get_ts(alarm)

    ack_time, clear_time, priority = inject_lifecycle(alarm, state)

    mtta_records.append({
        "alarm_id": alarm_id,
        "mtta_seconds": (ack_time - ts_open).total_seconds(),
        "priority": priority,
        "decision": state["decision"]
    })

    mttr_records.append({
        "alarm_id": alarm_id,
        "mttr_seconds": (clear_time - ts_open).total_seconds(),
        "priority": priority,
        "root_type": state.get("root_type"),
        "decision": state["decision"]
    })

# -----------------------------
# GRAPH NODES
# -----------------------------

def flap_node(state):
    return flapping_agent(state)

def topo_node(state):
    return topology_agent(state)

def decision_node(state):

    alarm = state["alarm"]

    if state["topology_suppressed"]:
        state["decision"] = "SUPPRESS_TOPOLOGY"
    elif state["is_flapping"]:
        state["decision"] = "SUPPRESS_FLAPPING"
    elif alarm.get("orchestrator_action") == "MONITOR_CORRELATED":
        state["decision"] = "MONITOR_ONLY"
    else:
        state["decision"] = "CREATE_INCIDENT"

    return state

def comm_node(state):

    msg = communication_agent(state)

    if msg:
        communication_logs.append(msg)

    return state

def orchestrator_node(state):

    alarm = state["alarm"]

    decisions.append({
        "alarm_id": alarm["alarm_id"],
        "device": alarm["device_name"],
        "decision": state["decision"],
        "root": state["root_device"],
        "impact": state["impact_radius"]
    })

    return state

# -----------------------------
# BUILD GRAPH
# -----------------------------

workflow = StateGraph(AlarmState)

workflow.add_node("flap", flap_node)
workflow.add_node("topo", topo_node)
workflow.add_node("decision", decision_node)
workflow.add_node("comm", comm_node)
workflow.add_node("orchestrator", orchestrator_node)

workflow.set_entry_point("flap")

workflow.add_edge("flap", "topo")
workflow.add_edge("topo", "decision")
workflow.add_edge("decision", "comm")
workflow.add_edge("comm", "orchestrator")

app = workflow.compile()

# -----------------------------
# LOAD DATA
# -----------------------------

def load_jsonl(path):
    data = []
    with open(path) as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

# -----------------------------
# RUN
# -----------------------------

def run():

    alarms = load_jsonl("data/orchestratedalarms_output.jsonl")
    alarms = sorted(alarms, key=get_ts)

    for alarm in alarms:

        state = {
            "alarm": alarm,
            "is_flapping": False,
            "topology_suppressed": False,
            "root_device": None,
            "root_type": None,
            "impact_radius": 0,
            "decision": ""
        }

        state = app.invoke(state)

        update_metrics(alarm, state)

    json.dump(decisions, open("results_27_03_2026/alarm_decisions.json", "w"), indent=2)
    json.dump(communication_logs, open("results_27_03_2026/communication_logs.json", "w"), indent=2)
    json.dump(mtta_records, open("results_27_03_2026/mtta_results.json", "w"), indent=2)
    json.dump(mttr_records, open("results_27_03_2026/mttr_results.json", "w"), indent=2)

if __name__ == "__main__":
    run()