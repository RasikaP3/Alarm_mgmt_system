# import json
# import time
# from datetime import datetime, timezone
# from kafka import KafkaConsumer
# from pymongo import MongoClient
# from pathlib import Path

# # -----------------------------
# # CONFIG
# # -----------------------------
# KAFKA_BROKER = "localhost:9092"
# INPUT_TOPIC = "classifiedalarms"

# MONGO_URI = "mongodb://localhost:27017/"
# DB_NAME = "noc_alarm_system"

# BASE_DIR = Path(__file__).resolve().parent.parent.parent
# OUTPUT_FILE = BASE_DIR / "data/orchestratedalarms_output.jsonl"   # ✅ better name

# # -----------------------------
# # INIT
# # -----------------------------
# consumer = KafkaConsumer(
#     INPUT_TOPIC,
#     bootstrap_servers=[KAFKA_BROKER],
#     group_id="orchestrator-group",   # ✅ prevent duplicate reads
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )

# client = MongoClient(MONGO_URI)
# db = client[DB_NAME]
# orchestrated_col = db["orchestrated_alarms"]

# # -----------------------------
# # CORE LOGIC
# # -----------------------------
# def orchestrate_alarm(alarm):

#     open_alarms_count = orchestrated_col.count_documents({
#         "device_name": alarm.get("device_name"),
#         "status": {"$ne": "RESOLVED"}
#     })

#     severity = alarm.get("severity", "low")

#     sla_action = "MONITOR"
#     if severity == "critical":
#         sla_action = "IMMEDIATE_ACTION"
#     elif severity == "major":
#         sla_action = "ESCALATE"

#     retry_count = alarm.get("retry_count", 0)
#     if retry_count > 3:
#         sla_action = "ESCALATE_TO_HUMAN"

#     audit_log = {
#         "received_at": datetime.now(timezone.utc).isoformat(),
#         "decision": sla_action,
#         "reason": alarm.get("reasoning")
#     }

#     orchestrated_alarm = {
#         **alarm,
#         "open_alarms_on_device": open_alarms_count,
#         "sla_action": sla_action,
#         "retry_count": retry_count,
#         "orchestrator_audit": audit_log,
#         "orchestratedAt": datetime.now(timezone.utc).isoformat()
#     }

#     return orchestrated_alarm


# # -----------------------------
# # MAIN LOOP
# # -----------------------------
# def run_orchestrator():
#     print("🚀 Orchestrator Agent Started...")

#     IDLE_TIMEOUT = 10
#     last_message_time = time.time()
#     count = 0

#     try:
#         with open(OUTPUT_FILE, "w") as out:
#             while True:
#                 records = consumer.poll(timeout_ms=1000)

#                 if records:
#                     for tp, msgs in records.items():
#                         for msg in msgs:
#                             alarm = msg.value

#                             try:
#                                 orchestrated = orchestrate_alarm(alarm)

#                                 # ❗ Remove _id if exists
#                                 orchestrated.pop("_id", None)

#                                 # ✅ Upsert (avoid duplicates)
#                                 orchestrated_col.update_one(
#                                     {"alarm_id": orchestrated["alarm_id"]},
#                                     {"$set": orchestrated},
#                                     upsert=True
#                                 )

#                                 # ✅ WRITE TO FILE
#                                 out.write(json.dumps(orchestrated) + "\n")
#                                 out.flush()

#                                 count += 1
#                                 print(f"🧠 Orchestrated alarm {orchestrated['alarm_id']} ({count})")

#                                 last_message_time = time.time()

#                             except Exception as e:
#                                 print("❌ Error in orchestrator:", e)

#                 else:
#                     if time.time() - last_message_time > IDLE_TIMEOUT:
#                         print("🛑 No messages. Stopping orchestrator...")
#                         break

#     except Exception as e:
#         print("❌ Fatal error:", e)

#     finally:
#         consumer.close()
#         print("✅ Orchestrator completed")


# if __name__ == "__main__":
#     run_orchestrator()

import json
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pathlib import Path

# ---------------- CONFIG ----------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "communicatedalarms"
OUTPUT_TOPIC = "orchestratedalarms"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "noc_alarm_system"

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_FILE = BASE_DIR / "data/orchestratedalarms_output.jsonl"

# ---------------- INIT ----------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="orchestrator-group",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
orchestrated_col = db["orchestrated_alarms"]

# ---------------- CORE LOGIC ----------------

def determine_action(alarm):

    severity = alarm.get("severity")
    priority = alarm.get("priority")
    impact = alarm.get("impact_score", 0)
    correlation = alarm.get("correlation_score", 0)
    is_correlated = alarm.get("is_correlated", False)

    # Base decision
    if priority == "P1" and severity == "critical":
        action = "IMMEDIATE_ACTION"
    elif impact >= 0.7 or correlation >= 0.8:
        action = "ESCALATE"
    elif is_correlated:
        action = "MONITOR_CORRELATED"
    else:
        action = "MONITOR"

    return action


def enrich_topology(alarm):

    parent = alarm.get("parent_device")
    children = alarm.get("child_alarms", [])

    topology_context = {
        "parent_device": parent,
        "child_devices": children,
        "topology_impact": "HIGH" if parent else "LOW"
    }

    return topology_context


def orchestrate_alarm(alarm):

    action = determine_action(alarm)
    topology = enrich_topology(alarm)

    # Count related open alarms (same correlation_id)
    correlation_id = alarm.get("correlation_id")

    related_count = orchestrated_col.count_documents({
        "correlation_id": correlation_id,
        "status": {"$ne": "RESOLVED"}
    })

    # Retry logic
    retry_count = alarm.get("retry_count", 0)
    if retry_count > 3:
        action = "ESCALATE_TO_HUMAN"

    # SLA tagging
    sla_level = "L3"
    if alarm.get("priority") == "P1":
        sla_level = "L1"
    elif alarm.get("priority") == "P2":
        sla_level = "L2"

    orchestrated_alarm = {
        **alarm,
        "orchestrator_action": action,
        "sla_level": sla_level,
        "related_alarms_count": related_count,
        "topology_context": topology,
        "retry_count": retry_count,
        "orchestratedAt": datetime.now(timezone.utc).isoformat(),
        "status": "OPEN",
        "decision_reason": f"Action={action}, Impact={alarm.get('impact_score')}, Correlation={alarm.get('correlation_score')}"
    }

    return orchestrated_alarm


# ---------------- MAIN LOOP ----------------

def run_orchestrator():

    print("🚀 Orchestrator Agent Started...")

    IDLE_TIMEOUT = 10
    last_message_time = time.time()
    count = 0

    try:
        with open(OUTPUT_FILE, "w") as out:

            while True:
                records = consumer.poll(timeout_ms=1000)

                if records:
                    for tp, msgs in records.items():
                        for msg in msgs:
                            alarm = msg.value

                            try:
                                orchestrated = orchestrate_alarm(alarm)

                                orchestrated.pop("_id", None)

                                # Save to MongoDB
                                orchestrated_col.update_one(
                                    {"alarm_id": orchestrated["alarm_id"]},
                                    {"$set": orchestrated},
                                    upsert=True
                                )

                                # Write to file
                                out.write(json.dumps(orchestrated) + "\n")
                                out.flush()

                                # Send to next stage
                                producer.send(OUTPUT_TOPIC, orchestrated)
                                producer.flush()

                                count += 1
                                print(f"🧠 Orchestrated: {orchestrated['alarm_id']} ({count})")

                                last_message_time = time.time()

                            except Exception as e:
                                print("❌ Orchestrator error:", e)

                else:
                    if time.time() - last_message_time > IDLE_TIMEOUT:
                        print("🛑 No messages. Stopping orchestrator...")
                        break

    finally:
        consumer.close()
        print("✅ Orchestrator completed")


if __name__ == "__main__":
    run_orchestrator()