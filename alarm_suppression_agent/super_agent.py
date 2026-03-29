import json
from datetime import datetime, timedelta
from typing import TypedDict, Optional

from langgraph.graph import StateGraph

from flapping_agent import flapping_agent
from topology_agent import topology_agent
from communication_agent import communication_agent

# -----------------------------------
# STORAGE
# -----------------------------------

decisions = []
communication_logs = []
mtta_records = []
mttr_records = []
enriched_alarms = []

# -----------------------------------
# STATE
# -----------------------------------

class AlarmState(TypedDict):
    alarm: dict
    is_flapping: bool
    topology_suppressed: bool
    root_device: Optional[str]
    impact_radius: int
    decision: str

# -----------------------------------
# UTILS
# -----------------------------------

def get_ts(alarm):
    return datetime.fromisoformat(alarm["createdAt"].replace("Z",""))

# -----------------------------------
# LIFECYCLE (NO SLA)
# -----------------------------------

def inject_lifecycle(alarm, decision):

    ts_open = get_ts(alarm)

    if decision == "SUPPRESS_FLAPPING":
        ack = ts_open + timedelta(seconds=10)
        clear = ts_open + timedelta(seconds=30)

    elif decision == "SUPPRESS_TOPOLOGY":
        ack = ts_open + timedelta(seconds=30)
        clear = ts_open + timedelta(seconds=120)

    elif decision == "CREATE_INCIDENT":
        ack = ts_open + timedelta(seconds=60)
        clear = ts_open + timedelta(seconds=300)

    else:
        return None, None

    return ack, clear

# -----------------------------------
# METRICS
# -----------------------------------

def update_metrics(alarm, decision):

    alarm_id = alarm["alarm_id"]
    ts_open = get_ts(alarm)

    ack, clear = inject_lifecycle(alarm, decision)

    if ack is None:
        return

    mtta_records.append({
        "alarm_id": alarm_id,
        "mtta_seconds": (ack - ts_open).total_seconds(),
        "decision": decision
    })

    mttr_records.append({
        "alarm_id": alarm_id,
        "mttr_seconds": (clear - ts_open).total_seconds(),
        "decision": decision
    })

# -----------------------------------
# GRAPH NODES
# -----------------------------------

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

    record = {
        "alarm_id": alarm["alarm_id"],
        "device": alarm["device_name"],
        "decision": state["decision"],
    }

    
    # Include impact ONLY when needed
    if state["decision"] in ["SUPPRESS_TOPOLOGY", "CREATE_INCIDENT"]:
        record["impact"] = state["impact_radius"]
    
    if state["root_device"] is not None:
        record["root"] = state["root_device"]

    decisions.append(record)

    return state

# -----------------------------------
# BUILD GRAPH
# -----------------------------------

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

# -----------------------------------
# LOAD
# -----------------------------------

def load_jsonl(path):
    data = []
    with open(path) as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

# -----------------------------------
# RUN
# -----------------------------------

import os
os.makedirs('results_29_03_2026', exist_ok=True)

def run():

    alarms = load_jsonl("data/orchestratedalarms_output.jsonl")
    alarms = sorted(alarms, key=get_ts)

    for alarm in alarms:

        state = {
            "alarm": alarm,
            "is_flapping": False,
            "topology_suppressed": False,
            "root_device": None,
            "impact_radius": 0,
            "decision": ""
        }

        state = app.invoke(state)

        # Metrics
        update_metrics(alarm, state["decision"])

        # Enriched output
        ack, clear = inject_lifecycle(alarm, state["decision"])

        enriched = alarm.copy()
        enriched["decision"] = state["decision"]

        if ack:
            enriched["acknowledged_at"] = ack.isoformat()

        if clear:
            enriched["cleared_at"] = clear.isoformat()

        enriched_alarms.append(enriched)

    # SAVE FILES
    json.dump(decisions, open("results_29_03_2026/alarm_decisions.json", "w"), indent=2)
    json.dump(communication_logs, open("results_29_03_2026/communication_logs.json", "w"), indent=2)
    json.dump(mtta_records, open("results_29_03_2026/mtta_results.json", "w"), indent=2)
    json.dump(mttr_records, open("results_29_03_2026/mttr_results.json", "w"), indent=2)
    json.dump(enriched_alarms, open("results_29_03_2026/enriched_alarms.json", "w"), indent=2)

if __name__ == "__main__":
    run()