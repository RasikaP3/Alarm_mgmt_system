import json
import uuid
import pymongo
from datetime import datetime, timezone
from kafka import KafkaConsumer
from langchain_huggingface import HuggingFaceEndpoint
import os

os.environ["HUGGINGFACEHUB_API_TOKEN"] = "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"

# ---------------------------------
# CONFIG
# ---------------------------------

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "structured-alarms"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "noc_db"
COLLECTION = "classified_alarms"

OUTPUT_FILE = "classified_alarms_output.jsonl"

# ---------------------------------
# TOPOLOGY
# ---------------------------------

TOPOLOGY = {
    "DEV-AGG-1": None,
    "DEV-AGG-2": None,
    "DEV-ACC-1": "DEV-AGG-1",
    "DEV-ACC-2": "DEV-AGG-1",
    "DEV-ACC-3": "DEV-AGG-2"
}

active_root_causes = set()
seen_events = set()

# ---------------------------------
# MONGODB
# ---------------------------------

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

# ---------------------------------
# FILE OUTPUT
# ---------------------------------

def write_alarm_to_file(alarm):

    alarm_copy = dict(alarm)

    if "_id" in alarm_copy:
        alarm_copy["_id"] = str(alarm_copy["_id"])

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(alarm_copy) + "\n")

# ---------------------------------
# KAFKA
# ---------------------------------

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Classification Agent Started")

# ---------------------------------
# LLM
# ---------------------------------

llm = HuggingFaceEndpoint(
    repo_id="mistralai/Mistral-7B-Instruct-v0.3",
    task="text-generation",
    temperature=0.1,
    max_new_tokens=200
)

# ---------------------------------
# CLASSIFICATION
# ---------------------------------

def classify_alarm(alarm):

    msg = alarm.get("event", {}).get("message", "N/A")

    prompt = f"""
Classify this NOC alarm.

Categories:
Link Failure
Hardware Failure
Performance Degradation
Power Failure
Configuration Issue

Return JSON:
{{"category":"...", "confidence":0.9}}

Alarm: {msg}
"""

    try:

        response = llm.invoke(prompt)

        text = response

        start = text.find("{")
        end = text.rfind("}") + 1

        if start != -1:
            return json.loads(text[start:end])

    except Exception as e:
        print("LLM error:", e)

    return {"category": "Unknown", "confidence": 0.5}

# ---------------------------------
# DYNAMIC LIFECYCLE ENGINE
# ---------------------------------

def lifecycle_engine(alarm):

    now = datetime.now(timezone.utc).isoformat()

    device_id = alarm["device"]["id"]
    event_type = alarm["event"]["type"]

    severity = alarm.get("severity", {}).get("adjusted", "MINOR")

    key = f"{device_id}-{event_type}"

    parent = TOPOLOGY.get(device_id)

    # -------------------------
    # CLEAR / RESOLVE
    # -------------------------

    if severity in ["CLEAR", "NORMAL"]:

        if device_id in active_root_causes:
            active_root_causes.remove(device_id)

        if key in seen_events:
            seen_events.remove(key)

        alarm["status"] = "CLEARED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "acknowledgedAt": now,
            "clearedAt": now
        }

        return alarm

    # -------------------------
    # DUPLICATE
    # -------------------------

    if key in seen_events:

        alarm["status"] = "SUPPRESSED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "suppressedAt": now
        }

        return alarm

    seen_events.add(key)

    # -------------------------
    # TOPOLOGY SUPPRESSION
    # -------------------------

    if parent in active_root_causes:

        alarm["status"] = "SUPPRESSED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "suppressedAt": now
        }

        return alarm

    # -------------------------
    # ROOT CAUSE
    # -------------------------

    if severity == "CRITICAL":

        active_root_causes.add(device_id)

        alarm["status"] = "ACKNOWLEDGED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "acknowledgedAt": now,
            "clearedAt": None
        }

    else:

        alarm["status"] = "CLEARED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "acknowledgedAt": now,
            "clearedAt": now
        }

    return alarm

# ---------------------------------
# MAIN LOOP
# ---------------------------------

for message in consumer:

    alarm = message.value

    device = alarm.get("device", {}).get("id", "unknown")

    alarm["classificationId"] = str(uuid.uuid4())

    classification = classify_alarm(alarm)

    alarm["classification"] = classification

    alarm = lifecycle_engine(alarm)

    # Save Mongo
    collection.insert_one(alarm)

    # Save JSON
    write_alarm_to_file(alarm)

    icon = {
        "SUPPRESSED": "🔇",
        "ACKNOWLEDGED": "🚨",
        "CLEARED": "✅"
    }.get(alarm["status"], "🔔")

    print(
        f"{icon} Device: {device} | "
        f"Status: {alarm['status']} | "
        f"Category: {classification['category']}"
    )