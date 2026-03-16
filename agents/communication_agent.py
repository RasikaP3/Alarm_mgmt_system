import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timezone

# -----------------------------
# CONFIG
# -----------------------------
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "suppressed_alarms"
INCIDENT_TOPIC = "incident"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "noc_alarm_system"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

alarm_collection = db.processed_alarms
communication_collection = db.communication_logs

# -----------------------------
# KAFKA CONSUMER & PRODUCER
# -----------------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="communication-group"
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # <--- FIX: serialize datetime
)

print("📢 Communication Agent Listening to Kafka...")

# -----------------------------
# SAVE ALARM TO DATABASE
# -----------------------------
def save_alarm_to_db(payload):
    payload["stored_at"] = datetime.now(timezone.utc).isoformat()  # timezone-aware
    alarm_collection.insert_one(payload)

# -----------------------------
# SAVE COMMUNICATION LOG
# -----------------------------
def save_communication_log(log):
    log["created_at"] = datetime.now(timezone.utc).isoformat()  # timezone-aware
    communication_collection.insert_one(log)

# -----------------------------
# PROCESS MESSAGES
# -----------------------------
for message in consumer:
    alarm = message.value

    # Safely handle device
    device_obj = alarm.get("device") or {}
    if isinstance(device_obj, dict):
        device_id = device_obj.get("id")
        device_name = device_obj.get("name")
        location = device_obj.get("location")
        ip = device_obj.get("ip")
    else:
        device_id = device_obj
        location = None
        ip = None

    classification = alarm.get("classification") or {}
    lifecycle = alarm.get("lifecycle") or {}

    # Convert all datetime fields to ISO format
    created_at = lifecycle.get("createdAt")
    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()

    acknowledged_at = lifecycle.get("acknowledgedAt")
    if isinstance(acknowledged_at, datetime):
        acknowledged_at = acknowledged_at.isoformat()

    resolved_at = lifecycle.get("resolvedAt")
    if isinstance(resolved_at, datetime):
        resolved_at = resolved_at.isoformat()

    communication_payload = {
        "alarm_id": alarm.get("alarmId"),
        "alarm_type": alarm.get("alarmType"),
        "device_id": device_id,
        "device_name":device_name,
        "location": location,
        "ip": ip,
        "severity": alarm.get("severity"),
        "status": alarm.get("status"),
        "source": alarm.get("source"),
        "message": alarm.get("message"),
        "classification": {
            "category": classification.get("category"),
            "confidence": classification.get("confidence")
        },
        "services_affected": alarm.get("servicesAffected"),
        "customers_affected": alarm.get("customersAffected"),
        "created_at": created_at,
        "acknowledged_at": acknowledged_at,
        "resolved_at": resolved_at,
        "is_flapping": alarm.get("is_flapping", False),
        "topology_suppressed": alarm.get("topology_suppressed", False),
        "root_device": alarm.get("root_device"),
        "decision": alarm.get("decision"),
        "mtta_seconds": alarm.get("mtta_seconds"),
        "mttr_seconds": alarm.get("mttr_seconds")
    }

    # Save alarm to DB
    save_alarm_to_db(communication_payload)

    # Save communication log
    save_communication_log({
        "alarm_id": communication_payload["alarm_id"],
        "device": communication_payload["device_id"],
        "decision": communication_payload["decision"],
        "message": communication_payload["message"]
    })

    # Send payload to Kafka incident topic
    producer.send(INCIDENT_TOPIC, communication_payload)
    producer.flush()

    print(f"✅ Alarm {communication_payload['alarm_id']} sent to Kafka topic '{INCIDENT_TOPIC}' and stored in MongoDB\n")