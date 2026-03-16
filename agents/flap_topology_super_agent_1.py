import json
from datetime import datetime, timedelta
from collections import defaultdict
from typing import TypedDict, Optional
from pathlib import Path
from langgraph.graph import StateGraph
from pymongo import MongoClient
from kafka import KafkaProducer

BASE_DIR = Path(__file__).resolve().parent.parent

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

FLAP_WINDOW = timedelta(seconds=120)
FLAP_THRESHOLD = 3

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "noc_alarm_system"

KAFKA_BROKER = "localhost:9092"
SUPPRESSED_TOPIC = "suppressed_alarms"

# --------------------------------------------------
# MONGODB CONNECTION
# --------------------------------------------------

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --------------------------------------------------
# KAFKA PRODUCER
# --------------------------------------------------

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

# --------------------------------------------------
# LOAD TOPOLOGY FROM DB
# --------------------------------------------------

def load_topology_from_db():
    topology = {}
    edges = db.topology.find()
    for edge in edges:
        source = edge["source"].lower()
        dest = edge["destination"].lower()
        if dest not in topology:
            topology[dest] = []
        topology[dest].append(source)
    return topology

TOPOLOGY = load_topology_from_db()

# --------------------------------------------------
# GLOBAL MEMORY
# --------------------------------------------------

flap_memory = defaultdict(list)
active_root_devices = set()

communication_logs = []
decisions = []
mtta_records = []
mttr_records = []

# --------------------------------------------------
# STATE
# --------------------------------------------------

class AlarmState(TypedDict):
    alarm: dict
    is_flapping: bool
    topology_suppressed: bool
    root_device: Optional[str]
    decision: str

# --------------------------------------------------
# UTILITIES
# --------------------------------------------------

def load_jsonl(path):
    data = []
    with open(path, "r") as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

def get_alarm_timestamp(alarm):
    ts = alarm["timestamp"]
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def get_device_id(alarm):
    device = alarm.get("device", {})
    if isinstance(device, dict):
        return device.get("id", "").lower()
    if isinstance(device, str):
        return device.lower()
    return None

def get_severity(alarm):
    sev = alarm.get("severity")
    if isinstance(sev, dict):
        return sev.get("original")
    return sev

# --------------------------------------------------
# MERGE DATA
# --------------------------------------------------

def merge_alarm_data(monitoring_file, classified_file):
    monitoring = load_jsonl(monitoring_file)
    classified = load_jsonl(classified_file)
    classified_map = {c["alarmId"]: c for c in classified}
    merged = []
    for m in monitoring:
        alarm_id = m["alarmId"]
        c = classified_map.get(alarm_id, {})
        alarm = {**m, **c}
        alarm["alarm_id"] = alarm_id
        merged.append(alarm)
    return merged

# --------------------------------------------------
# FLAPPING AGENT
# --------------------------------------------------

def flapping_agent(state: AlarmState):
    alarm = state["alarm"]
    device = get_device_id(alarm)
    if not device:
        state["is_flapping"] = False
        return state
    ts = get_alarm_timestamp(alarm)
    history = flap_memory[device]
    history = [t for t in history if ts - t < FLAP_WINDOW]
    history.append(ts)
    flap_memory[device] = history
    state["is_flapping"] = len(history) >= FLAP_THRESHOLD
    return state

# --------------------------------------------------
# TOPOLOGY AGENT (DB BASED)
# --------------------------------------------------

def topology_agent(state: AlarmState):
    alarm = state["alarm"]
    device = get_device_id(alarm)
    severity = get_severity(alarm)
    parents = TOPOLOGY.get(device, [])
    suppressed = any(p in active_root_devices for p in parents)
    state["topology_suppressed"] = suppressed
    state["root_device"] = parents[0] if suppressed else None
    if severity and severity.lower() == "critical":
        if alarm["status"] in ["OPEN", "ACKNOWLEDGED"]:
            active_root_devices.add(device)
        if alarm["status"] in ["CLEARED", "RESOLVED"]:
            active_root_devices.discard(device)
    return state

# --------------------------------------------------
# SUPER AGENT
# --------------------------------------------------

def super_agent(state: AlarmState):
    if state["topology_suppressed"]:
        state["decision"] = "SUPPRESS_TOPOLOGY"
    elif state["is_flapping"]:
        state["decision"] = "SUPPRESS_FLAPPING"
    else:
        state["decision"] = "CREATE_INCIDENT"
    return state

# --------------------------------------------------
# COMMUNICATION AGENT
# --------------------------------------------------

def communication_agent(state: AlarmState):
    alarm = state["alarm"]
    device = get_device_id(alarm)
    if state["decision"] == "SUPPRESS_TOPOLOGY":
        communication_logs.append({
            "type": "topology",
            "device": device,
            "root_device": state["root_device"],
            "message": f"Root cause {state['root_device']} detected. Suppressing downstream alarms."
        })
    elif state["decision"] == "SUPPRESS_FLAPPING":
        communication_logs.append({
            "type": "flapping",
            "device": device,
            "message": "Repeated alarms detected. Flapping suppression applied."
        })
    return state

# --------------------------------------------------
# METRICS
# --------------------------------------------------

# def update_metrics(alarm):
#     lifecycle = alarm.get("lifecycle", {})
#     opened = lifecycle.get("createdAt")
#     ack = lifecycle.get("acknowledgedAt")
#     cleared = lifecycle.get("resolvedAt")
#     alarm_id = alarm["alarmId"]
#     if opened and ack:
#         opened_dt = datetime.fromisoformat(opened)
#         ack_dt = datetime.fromisoformat(ack)
#         mtta = (ack_dt - opened_dt).total_seconds()
#         mtta_records.append({
#             "alarm_id": alarm_id,
#             "mtta_seconds": mtta
#         })
#     if opened and cleared:
#         opened_dt = datetime.fromisoformat(opened)
#         clear_dt = datetime.fromisoformat(cleared)
#         mttr = (clear_dt - opened_dt).total_seconds()
#          # Attach directly to alarm for orchestrator
#         alarm["mtta_seconds"] = mtta
#         alarm["mttr_seconds"] = mttr

def update_metrics(alarm):
    lifecycle = alarm.get("lifecycle", {})
    opened = lifecycle.get("createdAt")
    ack = lifecycle.get("acknowledgedAt")
    cleared = lifecycle.get("resolvedAt")
    alarm_id = alarm["alarmId"]

    if opened and ack:
        opened_dt = datetime.fromisoformat(opened)
        ack_dt = datetime.fromisoformat(ack)
        alarm["mtta_seconds"] = (ack_dt - opened_dt).total_seconds()
    else:
        alarm["mtta_seconds"] = None

    if opened and cleared:
        opened_dt = datetime.fromisoformat(opened)
        cleared_dt = datetime.fromisoformat(cleared)
        alarm["mttr_seconds"] = (cleared_dt - opened_dt).total_seconds()
    else:
        alarm["mttr_seconds"] = None

# --------------------------------------------------
# ORCHESTRATOR AGENT (UPDATED)
# --------------------------------------------------

def orchestrator_agent(state: AlarmState):
    alarm = state["alarm"]
    device = alarm.get("device", {})
    event = alarm.get("event", {})
    print("alarm orchestration=>>>>>>>>>>>>",alarm)
    parents = TOPOLOGY.get(get_device_id(alarm), [])
    root_device = parents[0] if parents else None
    # Normalize fields to ensure nothing is blank
    structured_alarm = {
        "alarmId": alarm.get("alarmId"),
        "device": {
            "id": device.get("id"),
            "name": device.get("name"),
            "ip": device.get("ip"),
            "location": device.get("location"),
            "vendor": device.get("vendor")
        },
        "alarmType": event.get("type"),
        "severity": get_severity(alarm),
        "status": alarm.get("status"),
        "classification": alarm.get("classification"),
        "servicesAffected": alarm.get("servicesAffected"),
        "customersAffected": alarm.get("customersAffected"),
        "ip": device.get("ip"),
        "source": event.get("source"),
        "message": event.get("message"),
        "lifecycle": alarm.get("lifecycle", {}),
        "topology_parent": alarm.get("topology", {}).get("parentDevice"),
        "mtta_seconds": alarm.get("mtta_seconds"),
        "mttr_seconds": alarm.get("mttr_seconds"),
       # Agent outputs
        "is_flapping": state["is_flapping"],
        "topology_suppressed": state["topology_suppressed"],
        "root_device": root_device,
        "decision": state["decision"]
    }

    # Save locally for debugging / reporting
    decisions.append({
        "alarm_id": alarm.get("alarmId"),
        "device": device.get("id"),
        "decision": state["decision"],
        "flapping": state["is_flapping"],
        "topology": state["topology_suppressed"]
    })

    # Push to Kafka
    try:
        producer.send(SUPPRESSED_TOPIC, json.dumps(structured_alarm).encode("utf-8"))
        producer.flush()
        print(f"📤 Structured alarm sent to Kafka → {alarm['alarmId']}")
    except Exception as e:
        print(f"❌ Failed to send alarm {alarm['alarmId']} to Kafka: {e}")

    return state
# --------------------------------------------------
# BUILD GRAPH
# --------------------------------------------------

workflow = StateGraph(AlarmState)
workflow.add_node("flapping", flapping_agent)
workflow.add_node("topology", topology_agent)
workflow.add_node("super", super_agent)
workflow.add_node("communication", communication_agent)
workflow.add_node("orchestrator", orchestrator_agent)
workflow.set_entry_point("flapping")
workflow.add_edge("flapping", "topology")
workflow.add_edge("topology", "super")
workflow.add_edge("super", "communication")
workflow.add_edge("communication", "orchestrator")
app = workflow.compile()

# --------------------------------------------------
# PIPELINE
# --------------------------------------------------

def run_pipeline(classified_file, monitoring_file):
    alarms = merge_alarm_data(monitoring_file, classified_file)
    alarms = sorted(alarms, key=get_alarm_timestamp)
    for alarm in alarms:
        update_metrics(alarm)
        state = {
            "alarm": alarm,
            "is_flapping": False,
            "topology_suppressed": False,
            "root_device": None,
            "decision": ""
        }
        state = app.invoke(state)
    save_results()

# --------------------------------------------------
# SAVE OUTPUT
# --------------------------------------------------

def save_results():
    json.dump(decisions, open(BASE_DIR / "data/alarm_decisions.json", "w"), indent=2)
    json.dump(communication_logs, open(BASE_DIR / "data/communication_logs.json", "w"), indent=2)
    json.dump(mtta_records, open(BASE_DIR / "data/mtta_results.json", "w"), indent=2)
    json.dump(mttr_records, open(BASE_DIR / "data/mttr_results.json", "w"), indent=2)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":
    run_pipeline(
        classified_file=BASE_DIR / "data/classified_alarms_output.jsonl",
        monitoring_file=BASE_DIR / "data/monitoring_agent_output.jsonl"
    )
    