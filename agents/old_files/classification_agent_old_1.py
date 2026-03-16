import json
import uuid
import pymongo
import os

from datetime import datetime, timezone
from kafka import KafkaConsumer
from langchain_huggingface import HuggingFaceEndpoint

# ------------------------------------------------
# CONFIG
# ------------------------------------------------

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "structured-alarms"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "noc_db"
COLLECTION = "classified_alarms"

OUTPUT_FILE = "classified_alarms_output.jsonl"

os.environ["HUGGINGFACEHUB_API_TOKEN"] = "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"

# ------------------------------------------------
# TOPOLOGY
# ------------------------------------------------

TOPOLOGY = {
    "DEV-AGG-1": None,
    "DEV-AGG-2": None,
    "DEV-ACC-1": "DEV-AGG-1",
    "DEV-ACC-2": "DEV-AGG-1",
    "DEV-ACC-3": "DEV-AGG-2"
}

# ------------------------------------------------
# MONGODB
# ------------------------------------------------

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

# ------------------------------------------------
# FILE OUTPUT
# ------------------------------------------------

def write_alarm_to_file(alarm):

    alarm_copy = dict(alarm)

    if "_id" in alarm_copy:
        alarm_copy["_id"] = str(alarm_copy["_id"])

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(alarm_copy) + "\n")

# ------------------------------------------------
# KAFKA CONSUMER
# ------------------------------------------------

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("🚀 Classification Agent Started")

# ------------------------------------------------
# LLM MODEL
# ------------------------------------------------

llm = HuggingFaceEndpoint(
    repo_id="google/flan-t5-large",
    task="text2text-generation",
    temperature=0.1,
    max_new_tokens=100
)

# ------------------------------------------------
# LLM CLASSIFICATION
# ------------------------------------------------

def classify_alarm(alarm):

    message = alarm.get("event", {}).get("message", "unknown alarm")

    prompt = f"""
Classify the network alarm into one category.

Categories:
Link Failure
Hardware Failure
Performance Degradation
Power Failure
Configuration Issue

Return JSON:
{{"category":"...", "confidence":0.9}}

Alarm: {message}
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

# ------------------------------------------------
# ALARM STATE MACHINE
# ------------------------------------------------

class AlarmStateMachine:

    def __init__(self):
        self.active_root_causes = set()
        self.seen_events = {}

    def process(self, alarm):

        now = datetime.now(timezone.utc).isoformat()

        device_id = alarm["device"]["id"]
        event_type = alarm["event"]["type"]

        key = f"{device_id}-{alarm['event']['message']}"

        parent = TOPOLOGY.get(device_id)

        # ---------------------
        # CLEAR EVENT
        # ---------------------

        if event_type == "CLEAR":

            if device_id in self.active_root_causes:
                self.active_root_causes.remove(device_id)

            alarm["status"] = "CLEARED"

            alarm["lifecycle"] = {
                "clearedAt": now
            }

            return alarm

        # ---------------------
        # DUPLICATE
        # ---------------------

        if key in self.seen_events:

            alarm["status"] = "SUPPRESSED"

            alarm["lifecycle"] = {
                "suppressedAt": now
            }

            return alarm

        self.seen_events[key] = now

        # ---------------------
        # TOPOLOGY SUPPRESSION
        # ---------------------

        if parent in self.active_root_causes:

            alarm["status"] = "SUPPRESSED"

            alarm["lifecycle"] = {
                "suppressedAt": now
            }

            return alarm

        # ---------------------
        # NEW ROOT ALARM
        # ---------------------

        self.active_root_causes.add(device_id)

        alarm["status"] = "ACKNOWLEDGED"

        alarm["lifecycle"] = {
            "openedAt": alarm["timestamp"],
            "acknowledgedAt": now,
            "clearedAt": None
        }

        return alarm

# ------------------------------------------------
# INIT STATE MACHINE
# ------------------------------------------------

state_machine = AlarmStateMachine()

# ------------------------------------------------
# MAIN PROCESS LOOP
# ------------------------------------------------

for message in consumer:

    alarm = message.value

    device = alarm.get("device", {}).get("id", "unknown")

    alarm["classificationId"] = str(uuid.uuid4())

    # LLM classification
    classification = classify_alarm(alarm)

    alarm["classification"] = classification

    # lifecycle state machine
    alarm = state_machine.process(alarm)

    # store MongoDB
    collection.insert_one(alarm)

    # store JSON file
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