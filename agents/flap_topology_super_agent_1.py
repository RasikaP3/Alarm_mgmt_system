import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import TypedDict
from langgraph.graph import StateGraph
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pathlib import Path
import time

# ---------------- PATHS ----------------
BASE_DIR = Path(__file__).resolve().parent.parent

DECISION_FILE = BASE_DIR / "data/alarm_decisions.json"
COMM_FILE = BASE_DIR / "data/communication_logs.json"
MTTA_FILE = BASE_DIR / "data/mtta_results.json"
MTTR_FILE = BASE_DIR / "data/mttr_results.json"

communication_logs = []
decisions = []
mtta_records = []
mttr_records = []

# ---------------- CONFIG ----------------
FLAP_WINDOW = timedelta(seconds=120)
FLAP_THRESHOLD = 3
RESOLVE_WINDOW = timedelta(seconds=180)

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "classifiedalarms"
OUTPUT_TOPIC = "suppressedalarms"
CREATE_INCIDENT ="createIncident"
SUPRESS_TOPOLOGY = "suppressTopology"
SUPRESS_FLAPPING = "suppressFlapping"
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "noc_alarm_system"

# ---------------- DB ----------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# ---------------- KAFKA ----------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="flap-agent"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# ---------------- MEMORY ----------------
flap_memory = defaultdict(list)
last_seen = {}

# ---------------- STATE ----------------
class AlarmState(TypedDict):
    alarm: dict
    is_flapping: bool
    decision: str

# ---------------- UTILS ----------------
def now():
    return datetime.now(timezone.utc)

def get_ts(alarm):
    ts = alarm.get("createdAt")
    if ts:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return now()

# # ---------------- GET PARENT DEVICE ----------------
# def get_parent_device(device_name):
#     record = db.topology.find_one({"destination": device_name})
#     if record:
#         return record["source"]
#     return None

# # ---------------- IS TOPOLOGY SUPPRESSED ----------------
# def is_topology_suppressed(alarm):
#     device = alarm.get("device_name")
#     parent = get_parent_device(device)
#     if not parent:
#         return False
#     # Strict parent match
#     for key, last in last_seen.items():
#         device_from_key = key.split("_")[0]
#         if parent == device_from_key:
#             if now() - last < RESOLVE_WINDOW:
#                 return True
#     return False

# ---------------- LIFECYCLE ----------------
def update_lifecycle(alarm):
    lifecycle = alarm.setdefault("lifecycle", {})

    # ✅ Use Kafka timestamp if available
    kafka_ts = alarm.get("createdAt")
    if kafka_ts:
        # Ensure timezone-aware
        created_dt = datetime.fromisoformat(kafka_ts)
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
        created_iso = created_dt.isoformat()
    else:
        created_iso = now().isoformat()

    # Set createdAt only if not already set
    if "createdAt" not in lifecycle:
        lifecycle["createdAt"] = created_iso

    # ACK logic
    if alarm["status"] == "ACKNOWLEDGED":
        if "acknowledgedAt" not in lifecycle:
            lifecycle["acknowledgedAt"] = now().isoformat()

    # RESOLVED / CLEARED logic
    if alarm["status"] in ["RESOLVED", "CLEARED"]:
        lifecycle.setdefault("resolvedAt", now().isoformat())
        lifecycle.setdefault("acknowledgedAt", lifecycle["createdAt"])

    return alarm
# def update_lifecycle(alarm):
#     lifecycle = alarm.setdefault("lifecycle", {})
#     current = now().isoformat()
#     print("alarm status:", alarm)
#     if "createdAt" not in lifecycle:
#         lifecycle["createdAt"] = current

#     if alarm["status"] == "ACKNOWLEDGED":
#         if "acknowledgedAt" not in lifecycle:
#             lifecycle["acknowledgedAt"] = current

#     if alarm["status"] in ["RESOLVED", "CLEARED"]:
#         lifecycle.setdefault("resolvedAt", current)
#         lifecycle.setdefault("acknowledgedAt", lifecycle["createdAt"])

#     return alarm

# ---------------- METRICS ----------------
def update_metrics(alarm):
    lc = alarm.get("lifecycle", {})
    print("Lifecycle:", lc)
    opened = lc.get("createdAt")
    ack = lc.get("acknowledgedAt")
    resolved = lc.get("resolvedAt")

    if opened and ack:
        alarm["mtta_seconds"] = (
            datetime.fromisoformat(ack) - datetime.fromisoformat(opened)
        ).total_seconds()
    else:
        alarm["mtta_seconds"] = None

    if opened and resolved:
        alarm["mttr_seconds"] = (
            datetime.fromisoformat(resolved) - datetime.fromisoformat(opened)
        ).total_seconds()
    else:
        alarm["mttr_seconds"] = None

# ---------------- FLAPPING ----------------
def flapping_agent(state: AlarmState):
    alarm = state["alarm"]
    key = alarm.get("alarm_key")
    ts = get_ts(alarm)

    # Clean old timestamps
    history = flap_memory[key]
    history = [t for t in history if ts - t < FLAP_WINDOW]
    history.append(ts)
    flap_memory[key] = history

    is_flap = len(history) >= FLAP_THRESHOLD
    state["is_flapping"] = is_flap

    # Status logic
    if key not in last_seen:
        alarm["status"] = "OPEN"
    elif is_flap:
        alarm["status"] = "ACKNOWLEDGED"
    else:
        alarm["status"] = "OPEN"

    last_seen[key] = ts
    return state

# ---------------- RESOLUTION ----------------
def resolution_check(alarm):
    key = alarm.get("alarm_key")
    if key in last_seen:
        if now() - last_seen[key] > RESOLVE_WINDOW:
            alarm["status"] = "RESOLVED"

# ---------------- DECISION ----------------
# def decision_agent(state: AlarmState):
#     alarm = state["alarm"]

#     if state["is_flapping"]:
#         state["decision"] = "SUPPRESS_FLAPPING"
#     elif is_topology_suppressed(alarm):
#         state["decision"] = "SUPPRESS_TOPOLOGY"
#     else:
#         state["decision"] = "CREATE_INCIDENT"

#     return state

# ---------------- ENRICH LIFECYCLE & METRICS ----------------
def enrich_agent(state: AlarmState):
    alarm = state["alarm"]
    resolution_check(alarm)
    update_lifecycle(alarm)
    update_metrics(alarm)
    return state

# ---------------- SAVE RESULTS ----------------
def save_results():
    DECISION_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DECISION_FILE, "w") as f:
        json.dump(decisions, f, indent=2)
    with open(COMM_FILE, "w") as f:
        json.dump(communication_logs, f, indent=2)
    with open(MTTA_FILE, "w") as f:
        json.dump(mtta_records, f, indent=2)
    with open(MTTR_FILE, "w") as f:
        json.dump(mttr_records, f, indent=2)
    print("💾 All output files saved successfully")

# ---------------- ORCHESTRATOR ----------------
def orchestrator(state: AlarmState):
    alarm = state["alarm"]

    structured = {
        "alarm_id": alarm.get("alarm_id"),
        "alarm_key": alarm.get("alarm_key"),
        "status": alarm.get("status"),
        "message": alarm.get("message"),
        "severity": alarm.get("severity"),
        "lifecycle": alarm.get("lifecycle"),
        "mtta_seconds": alarm.get("mtta_seconds"),
        "mttr_seconds": alarm.get("mttr_seconds"),
        "is_flapping": state["is_flapping"],
        "decision": state["decision"]
    }

    # Save decisions
    decisions.append({
        "alarm_id": structured["alarm_id"],
        "device_name": alarm.get("device_name"),
        "decision": state["decision"],
        "flapping": state["is_flapping"]
    })

    # Communication logs
    if state["decision"] == "SUPPRESS_FLAPPING":
        communication_logs.append({
            "type": "flapping",
            "alarm_id": structured["alarm_id"],
            "message": "Flapping detected → suppressed"

        })

    if state["decision"] == "SUPPRESS_TOPOLOGY":
        communication_logs.append({
            "type": "topology",
            "alarm_id": structured["alarm_id"],
            "message": "Topology detected → suppressed"
        })  

    # Metrics
    if structured["mtta_seconds"] is not None:
        mtta_records.append({
            "alarm_id": structured["alarm_id"],
            "mtta_seconds": structured["mtta_seconds"]
        })

    if structured["mttr_seconds"] is not None:
        mttr_records.append({
            "alarm_id": structured["alarm_id"],
            "mttr_seconds": structured["mttr_seconds"]
        })

    # Send to Kafka
    producer.send(OUTPUT_TOPIC, structured)
    producer.flush()

    print("📤 Sent:", structured["alarm_id"], structured["status"])
    return state

# ---------------- WORKFLOW ----------------
workflow = StateGraph(AlarmState)
workflow.add_node("flap", flapping_agent)
workflow.add_node("decision", decision_agent)
workflow.add_node("enrich", enrich_agent)
workflow.add_node("orchestrator", orchestrator)

workflow.set_entry_point("flap")
workflow.add_edge("flap", "decision")
workflow.add_edge("decision", "enrich")
workflow.add_edge("enrich", "orchestrator")

app = workflow.compile()

# ---------------- MAIN LOOP ----------------
print("🚀 Flapping Agent Listening...")

IDLE_TIMEOUT = 10
last_message_time = time.time()

try:
    while True:
        records = consumer.poll(timeout_ms=1000)
        if records:
            for tp, msgs in records.items():
                for msg in msgs:
                    alarm = msg.value

                    state = {
                        "alarm": alarm,
                        "is_flapping": False,
                        "decision": ""
                    }

                    state = app.invoke(state)
                    last_message_time = time.time()

        else:
            if time.time() - last_message_time > IDLE_TIMEOUT:
                print("🛑 No messages for a while. Exiting...")
                break

finally:
    consumer.close()
    save_results()
    print("✅ Flapping agent completed and files saved")