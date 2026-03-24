import json
import time
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timezone
from pathlib import Path

# ---------------- CONFIG ----------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "classifiedalarms"
OUTPUT_TOPIC = "communicatedalarms"

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_FILE = BASE_DIR / "data/communicatedalarms_output.jsonl"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "noc_alarm_system"

# ---------------- KAFKA ----------------
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="communication-group",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- MONGO ----------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
comm_col = db["communications"]

# ---------------- LLM / MESSAGE GENERATION ----------------

def generate_summary(alarm):
    return (
        f"{alarm.get('category')} issue on {alarm.get('device_name')} "
        f"({alarm.get('device_type')}) with severity {alarm.get('severity')}."
    )

def generate_executive_brief(alarm):
    return (
        f"Incident {alarm.get('alarm_type')} affecting "
        f"{alarm.get('customers_affected')} customers and "
        f"{alarm.get('services_affected')} services. "
        f"Priority: {alarm.get('priority')}."
    )

def generate_root_cause_note(alarm):
    rc = alarm.get("root_cause", "unknown")
    prob = alarm.get("root_cause_probability", 0)
    return f"Root cause identified as '{rc}' with probability {prob}."

def generate_correlation_note(alarm):
    if alarm.get("is_correlated"):
        return (
            f"Correlated with upstream/downstream dependency. "
            f"Reason: {alarm.get('correlation_reason')} "
            f"(Score: {alarm.get('correlation_score')})"
        )
    return "No correlation detected."

def generate_customer_message(alarm):
    severity = alarm.get("severity", "minor")

    if severity == "critical":
        return (
            f"Dear Customer, we are currently experiencing a critical issue "
            f"impacting your services. Our team is actively working on resolution."
        )
    elif severity == "major":
        return (
            f"Dear Customer, we are observing a service degradation. "
            f"We are working to resolve this shortly."
        )
    else:
        return (
            f"Dear Customer, a minor issue has been detected and is being monitored."
        )

def generate_priority_message(alarm):
    return (
        f"Priority: {alarm.get('priority')} | "
        f"Impact Score: {alarm.get('impact_score')} | "
        f"Category Confidence: {alarm.get('category_confidence')}"
    )

# ---------------- CORE ----------------

def process_communication(alarm):

    summary = generate_summary(alarm)
    executive_brief = generate_executive_brief(alarm)
    root_cause_note = generate_root_cause_note(alarm)
    correlation_note = generate_correlation_note(alarm)
    customer_message = generate_customer_message(alarm)
    priority_message = generate_priority_message(alarm)

    communicated = {
        **alarm,
        "summary": summary,
        "executive_brief": executive_brief,
        "root_cause_note": root_cause_note,
        "correlation_note": correlation_note,
        "customer_message": customer_message,
        "priority_message": priority_message,
        "communicatedAt": datetime.now(timezone.utc).isoformat(),
        "notifications_sent": True
    }

    return communicated

# ---------------- MAIN LOOP ----------------

def run_communication():
    print("📣 Communication Agent Started...")

    IDLE_TIMEOUT = 10
    last_message_time = time.time()

    try:
        with open(OUTPUT_FILE, "w") as out:
            while True:
                records = consumer.poll(timeout_ms=1000)

                if records:
                    for tp, msgs in records.items():
                        for msg in msgs:
                            alarm = msg.value

                            try:
                                communicated = process_communication(alarm)

                                # Remove MongoDB _id if present
                                communicated.pop("_id", None)

                                # Upsert into MongoDB
                                comm_col.update_one(
                                    {"alarm_id": communicated["alarm_id"]},
                                    {"$set": communicated},
                                    upsert=True
                                )

                                # Write to file
                                out.write(json.dumps(communicated) + "\n")
                                out.flush()

                                # Send to next stage
                                producer.send(OUTPUT_TOPIC, communicated)
                                producer.flush()

                                print(f"📣 Processed communication for {communicated.get('alarm_id')}")

                                last_message_time = time.time()

                            except Exception as e:
                                print("❌ Communication error:", e)

                else:
                    if time.time() - last_message_time > IDLE_TIMEOUT:
                        print("🛑 No messages. Stopping communication agent...")
                        break

    except KeyboardInterrupt:
        print("🛑 Interrupted by user")

    finally:
        consumer.close()
        print("✅ Communication agent completed")


if __name__ == "__main__":
    run_communication()