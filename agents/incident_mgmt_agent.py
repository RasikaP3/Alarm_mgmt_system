import json
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timezone
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "incident"
OUTPUT_TOPIC = "ticket_creation"

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "noc_alarm_system"

BASE_DIR = Path(__file__).resolve().parent.parent

OUTPUT_FILE = BASE_DIR / "data/incident_topic_output.jsonl"
OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)  # create folder if not exists

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

incident_collection = db.incidents  # Store incidents

# -----------------------------
# KAFKA SETUP
# -----------------------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="incident-mgmt-group"
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # serialize datetime safely
)

print("🚨 Incident Management Agent Listening...")

# -----------------------------
# PROCESS ALARMS
# -----------------------------
for message in consumer:
    alarm = message.value
    print("alarm-=?????????",alarm)
    alarm_id = alarm.get("alarm_id")
    logs = alarm.get("message")
    created_at = alarm.get("created_at") or datetime.now(timezone.utc).isoformat()
    classification = alarm.get("classification")
    device_id = alarm.get("device_id")
    device_name = alarm.get("device_name")
  
    # Check if incident already exists
    incident = incident_collection.find_one({"alarmID": alarm_id})

    if not incident:
        # Create new incident
        incident_payload = {
            "category": classification.get("category"),
            "alarmID": alarm_id,
            "alarmType": alarm.get("alarm_type"),
            "message": alarm.get("message"),
            "deviceID":alarm.get("device_id"),
            "devicename":alarm.get("device_name"),
            "location": alarm.get("location"),
            "source": alarm.get("source"),
            "createdAt": created_at,
            "services_affected": alarm.get("services_affected"),
            "priority": alarm.get("impact", {}).get("priority") or "P3",
            "logs": [logs],
            "acknowledgedAt": "null",
            "resolvedAt": "null"
        }

        # Save to DB
        incident_collection.insert_one(incident_payload)
        print(f"✅ Created new incident for alarm {alarm_id}")

    else:
        # Append logs if incident already exists
        updated_logs = incident.get("logs", [])
        updated_logs.append(logs)
        incident_collection.update_one(
            {"alarmID": alarm_id},
            {"$set": {
                "logs": updated_logs,
                "resolvedAt": alarm.get("resolved_at"),
                "acknowledgedAt": alarm.get("acknowledged_at")
            }}
        )
        incident_payload = incident_collection.find_one({"alarmID": alarm_id})
        print(f"ℹ️ Updated existing incident for alarm {alarm_id}")

    # -----------------------------
    # SEND TO TICKET CREATION TOPIC
    # -----------------------------
    producer.send(OUTPUT_TOPIC, incident_payload)
    producer.flush()
    print(f"📨 Incident for alarm {alarm_id} sent to Kafka topic '{OUTPUT_TOPIC}'")

    # -----------------------------
    # WRITE INCIDENT TO FILE
    # -----------------------------
    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(incident_payload, default=str) + "\n")
    print(f"📝 Incident for alarm {alarm_id} written to file '{OUTPUT_FILE}'\n")