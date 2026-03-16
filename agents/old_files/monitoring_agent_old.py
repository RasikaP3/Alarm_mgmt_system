import time
import os
import json
import uuid
import redis
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

# -----------------------------
# CONFIG
# -----------------------------
KAFKA_BOOTSTRAP = "localhost:9092"
RAW_TOPIC = "raw-events"
STRUCTURED_TOPIC = "structured-alarms"
OUTPUT_FILE = "monitoring_agent_output.jsonl"

REDIS_HOST = "localhost"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "noc_alarm_system"

# -----------------------------
# CONNECTIONS
# -----------------------------
# Initialize clients globally for reuse
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# LOAD JSON EVENTS → KAFKA
# -----------------------------
def load_events_to_kafka():
    """Reads local JSON file and pushes to the raw Kafka topic."""
    file_path = "data/network_events.json"

    if not os.path.exists(file_path):
        print(f"File {file_path} not found. Skipping initial load.")
        return

    print(f"Loading events from {file_path} → Kafka...")
    with open(file_path, 'r') as f:
        try:
            events = json.load(f)
            for event in events:
                producer.send(RAW_TOPIC, event)
            producer.flush() # Ensure all events are sent
            print(f"Successfully loaded {len(events)} events.")
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON file.")

# -----------------------------
# UTILS
# -----------------------------
def write_alarm_to_file(alarm):
    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(alarm) + "\n")

def get_now_iso():
    return datetime.now(timezone.utc).isoformat()

# -----------------------------
# PROCESSING LOGIC
# -----------------------------
def normalize_event(event):
    return {
        "event_id": event.get("event_id", str(uuid.uuid4())),
        "timestamp": event.get("timestamp", get_now_iso()),
        "source": event.get("source", "unknown"),
        "device_name": event.get("device_name"),
        "device_id": event.get("device_id"),
        "ip": event.get("ip"),
        "hostname": event.get("hostname"),
        "vendor": event.get("vendor"),
        "model": event.get("model"),
        "severity": event.get("severity", "INFO"),
        "event_type": event.get("event_type", "GENERIC_EVENT"),
        "message": event.get("message", ""),
        "cpu_utilization": event.get("cpu_utilization"),
        "memory_utilization": event.get("memory_utilization")
    }

def deduplicate(event):
    """Tracks event frequency in Redis to prevent alarm fatigue."""
    key = f"dedup:{event['device_id']}:{event['event_type']}"
    now = get_now_iso()

    record_raw = redis_client.get(key)
    
    if record_raw:
        record = json.loads(record_raw)
        record["dedupCount"] += 1
        record["lastSeen"] = now
    else:
        record = {
            "firstSeen": now,
            "lastSeen": now,
            "dedupCount": 1
        }

    # Set with an expiration (e.g., 1 hour) so Redis doesn't fill up forever
    redis_client.setex(key, 3600, json.dumps(record))
    return record

def enrich_alarm(alarm):
    """Queries MongoDB to add business context to the alarm."""
    device_id = alarm["device"]["id"]
    
    # Enrich Device Info
    device_doc = db.devices.find_one({"device_id": device_id})
    if device_doc:
        alarm["device"]["location"] = device_doc.get("location")
        alarm["device"]["role"] = device_doc.get("tier")

    # Impact Analysis: Find affected services and customers
    services = list(db.services.find({"devices": device_id}))
    alarm["servicesAffected"] = len(services)

    service_ids = [s["service_id"] for s in services]
    customers = list(db.customers.find({"service_id": {"$in": service_ids}}))
    alarm["customersAffected"] = len(customers)

    return alarm

def impact_and_severity_engine(alarm):
    """Calculates priority and adjusts severity based on customer impact."""
    c_count = alarm["customersAffected"]

    if c_count > 100:
        impact, priority = "HIGH", "P1"
    elif c_count > 20:
        impact, priority = "MEDIUM", "P2"
    else:
        impact, priority = "LOW", "P3"

    alarm["impact"] = {"businessImpact": impact, "priority": priority}

    # Elevate severity if impact is high
    if impact == "HIGH":
        alarm["severity"]["adjusted"] = "CRITICAL"
    elif impact == "MEDIUM" and alarm["severity"]["original"] != "CRITICAL":
        alarm["severity"]["adjusted"] = "MAJOR"
    else:
        alarm["severity"]["adjusted"] = alarm["severity"]["original"]

    return alarm

# -----------------------------
# MAIN PROCESSOR
# -----------------------------
def process_event(raw_event):
    event = normalize_event(raw_event)
    dedup = deduplicate(event)

    # Construct Canonical Alarm
    alarm = {
        "alarmId": str(uuid.uuid4()),
        "timestamp": event["timestamp"],
        "device": {
            "id": event["device_id"],
            "name": event["device_name"],
            "ip": event["ip"],
            "vendor": event["vendor"]
        },
        "event": {
            "type": event["event_type"],
            "message": event["message"]
        },
        "severity": {"original": event["severity"]},
        "status": "OPEN",
        "firstSeen": dedup["firstSeen"],
        "lastSeen": dedup["lastSeen"],
        "dedupCount": dedup["dedupCount"],
        "lifecycle": {"createdAt": get_now_iso()}
    }

    alarm = enrich_alarm(alarm)
    alarm = impact_and_severity_engine(alarm)
    
    return alarm

def start_agent():
    # Load any initial data
    load_events_to_kafka()

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("--- Monitoring Agent Started and Listening ---")
    try:
        for msg in consumer:
            alarm = process_event(msg.value)
            
            # Outbound
            producer.send(STRUCTURED_TOPIC, alarm)
            write_alarm_to_file(alarm)
            
            print(f"Processed Alarm: {alarm['alarmId']} | Priority: {alarm['impact']['priority']}")
    except KeyboardInterrupt:
        print("Shutting down agent...")

if __name__ == "__main__":
    start_agent()