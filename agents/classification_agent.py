import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

# ---------------- PATH ----------------
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "data" / "classifiedalarms_output.jsonl"

# ---------------- KAFKA ----------------
KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "structuredalarms"
OUTPUT_TOPIC = "classifiedalarms"

CREATE_INCIDENT = "createIncident"
SUPPRESS_TOPOLOGY = "suppressTopology"
SUPPRESS_FLAPPING = "suppressFlapping"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# ---------------- MONGO ----------------
client = MongoClient("mongodb://localhost:27017/")
db = client["noc_alarm_system"]
alarms_collection = db["alarms"]

# ---------------- CONFIG ----------------
FLAP_WINDOW = timedelta(seconds=120)
FLAP_THRESHOLD = 3
RESOLVE_WINDOW = timedelta(seconds=180)

# ---------------- MEMORY ----------------
flap_memory = defaultdict(list)
last_seen = {}

# ---------------- UTILS ----------------
def now():
    return datetime.now(timezone.utc)

def normalize_device_name(name):
    if not name:
        return name
    return name.split(".")[0].strip().lower()

def get_ts(alarm):
    ts = alarm.get("createdAt")
    if ts:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return now()

# ---------------- TOPOLOGY DB ----------------
def get_parent_device(device_name):
    device = normalize_device_name(device_name)

    record = db.topology.find_one({
        "destination": {"$regex": f"^{device}$", "$options": "i"}
    })

    return normalize_device_name(record.get("source")) if record else None


def get_child_devices(device_name):
    device = normalize_device_name(device_name)

    records = db.topology.find({
        "source": {"$regex": f"^{device}$", "$options": "i"}
    })

    return [
        normalize_device_name(r.get("destination"))
        for r in records if r.get("destination")
    ]

# ---------------- CLASSIFICATION ----------------
def classify_alarm(alarm):
    event = alarm.get("event", {})

    alarm_type = str(event.get("type") or alarm.get("alarm_type") or "").lower()
    message = str(event.get("message") or alarm.get("message") or "").lower()

    device_name = normalize_device_name(alarm.get("device_name"))

    # 🔥 Topology lookup
    parent_alarm = get_parent_device(device_name)
    child_alarms = get_child_devices(device_name)

    # Debug
    print(f"🔍 Device: {device_name} | Parent: {parent_alarm} | Children: {child_alarms}")

    category = "unknown"
    confidence = 0.5
    root_cause = "unknown"
    status = "OPEN"

    if "link" in alarm_type or "link" in message:
        category = "link_failure"
        confidence = 0.9
        root_cause = "Fiber/backbone issue"

    elif "hardware" in alarm_type or "hw" in message:
        category = "hardware_failure"
        confidence = 0.9
        root_cause = "Hardware failure"

    elif "config" in alarm_type or "configuration" in message:
        category = "configuration_issue"
        confidence = 0.85
        root_cause = "Configuration mismatch"

    elif "power" in alarm_type:
        category = "power_failure"
        confidence = 0.9
        root_cause = "Power issue"

    elif "memory" in message or "cpu" in message:
        category = "resource_issue"
        confidence = 0.8
        root_cause = "Resource utilization issue"

    elif "performance" in message:
        category = "performance_degradation"
        confidence = 0.8
        root_cause = "High load"

    elif "resolved" in message:
        category = "resolved"
        status = "RESOLVED"

    return {
        "alarm_id": alarm.get("alarm_id"),
        "alarm_key": f"{device_name}_{alarm_type}",
        "device_name": device_name,
        "device_type": alarm.get("device_type"),
        "parent_alarm": parent_alarm,
        "child_alarms": child_alarms,
        "message": message,
        "category": category,
        "severity": alarm.get("severity"),
        "category_confidence": confidence,
        "root_cause": root_cause,
        "createdAt": alarm.get("createdAt"),
        "status": status,
    }

# ---------------- FLAPPING ----------------
def flapping_logic(alarm):
    key = alarm.get("alarm_key") or alarm.get("alarm_id")
    ts = get_ts(alarm)

    history = flap_memory[key]
    history = [t for t in history if ts - t < FLAP_WINDOW]
    history.append(ts)
    flap_memory[key] = history

    is_flap = len(history) >= FLAP_THRESHOLD

    if is_flap:
        alarm["status"] = "ACKNOWLEDGED"
    else:
        alarm["status"] = "OPEN"

    last_seen[key] = ts
    return is_flap

# ---------------- TOPOLOGY SUPPRESSION ----------------
def is_topology_suppressed(alarm):
    parent = alarm.get("parent_alarm")
    if not parent:
        return False

    for key, last in last_seen.items():
        if key.startswith(parent):
            if now() - last < RESOLVE_WINDOW:
                return True
    return False

# ---------------- RESOLUTION ----------------
def resolution_check(alarm):
    key = alarm.get("alarm_key")
    if key in last_seen:
        if now() - last_seen[key] > RESOLVE_WINDOW:
            alarm["status"] = "RESOLVED"

# ---------------- LIFECYCLE ----------------
def update_lifecycle(alarm):
    lifecycle = alarm.setdefault("lifecycle", {})
    current = now().isoformat()

    lifecycle.setdefault("createdAt", alarm.get("createdAt") or current)
    lifecycle["classificationAt"] = current

    if alarm["status"] == "ACKNOWLEDGED":
        lifecycle.setdefault("acknowledgedAt", current)

    if alarm["status"] == "RESOLVED":
        lifecycle.setdefault("resolvedAt", current)

    return alarm

# ---------------- DECISION ----------------
def decide(alarm, is_flap):
    if is_flap:
        return SUPPRESS_FLAPPING
    elif is_topology_suppressed(alarm):
        return SUPPRESS_TOPOLOGY
    else:
        return CREATE_INCIDENT

# ---------------- MAIN ----------------
def run_classification():
    print(f"🚀 Listening to '{INPUT_TOPIC}'...")

    IDLE_TIMEOUT = 10
    last_message_time = time.time()
    count = 0

    with open(OUTPUT_FILE, "a") as f:
        while True:
            records = consumer.poll(timeout_ms=1000)

            if records:
                for _, msgs in records.items():
                    for msg in msgs:
                        try:
                            alarm = msg.value

                            # 1. Classification
                            classified = classify_alarm(alarm)

                            # 2. Flapping
                            is_flap = flapping_logic(classified)

                            # 3. Decision
                            decision = decide(classified, is_flap)
                            classified["decision"] = decision
                            classified["is_flapping"] = is_flap

                            # 4. Resolution
                            resolution_check(classified)

                            # 5. Lifecycle
                            classified = update_lifecycle(classified)

                            # 6. MongoDB
                            result = alarms_collection.insert_one(classified)
                            classified["_id"] = str(result.inserted_id)

                            # 7. File
                            f.write(json.dumps(classified) + "\n")

                            # 8. Kafka Routing
                            if decision == SUPPRESS_FLAPPING:
                                producer.send(SUPPRESS_FLAPPING, classified)

                            elif decision == SUPPRESS_TOPOLOGY:
                                producer.send(SUPPRESS_TOPOLOGY, classified)

                            else:
                                producer.send(CREATE_INCIDENT, classified)

                            producer.send(OUTPUT_TOPIC, classified)
                            producer.flush()

                            count += 1
                            print(f"✅ {classified['alarm_id']} → {decision} ({count})")

                            last_message_time = time.time()

                        except Exception as e:
                            print("❌ Error:", e)

            else:
                if time.time() - last_message_time > IDLE_TIMEOUT:
                    print("🛑 No messages. Exiting...")
                    break

    consumer.close()
    print("✅ Agent stopped")


if __name__ == "__main__":
    run_classification()