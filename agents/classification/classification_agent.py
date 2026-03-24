from utils.faiss_store import FAISSStore
# -----------------------------
# INIT FAISS STORE
# -----------------------------
faiss_store = FAISSStore()

import json
import uuid
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from datetime import datetime, timezone
import time

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_FILE = BASE_DIR / "data/classifiedalarms_output.jsonl"

# -----------------------------
# Kafka Configuration
# -----------------------------
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "structuredalarms"
OUTPUT_TOPIC = "classifiedalarms"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -----------------------------
# MongoDB Configuration
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["noc_alarm_system"]
alarms_collection = db["alarms"]



# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
SIMILARITY_THRESHOLD = 0.75

def is_duplicate(alarm, similar_alarms, threshold=0.95):
    for a in similar_alarms:
        if a.get("score", 0) >= threshold:
            return True
    return False


# -----------------------------
# CLASSIFICATION FUNCTION
# -----------------------------
def classify_alarm(alarm):

    alarm_type = str(alarm.get("alarm_type", "")).lower()
    normalized_type = alarm.get("normalized_alarm_type", "")
    message = str(alarm.get("message", "")).lower()
    device_name = alarm.get("device_name", "")
    device_type = alarm.get("device_type", "")
    severity = alarm.get("adjusted_severity", "low").lower()

    impact_score = alarm.get("impact_score", 0)
    priority = alarm.get("priority", "P3")
    is_anomaly = alarm.get("is_anomaly", False)

    services_affected = alarm.get("services_affected", 0)
    customers_affected = alarm.get("customers_affected", 0)

    parent_device = alarm.get("parent_device")
    neighbors = alarm.get("topology_neighbors", [])

    text = alarm_type + " " + message

    # DEFAULT VALUES
    category = "unknown"
    confidence = 0.5
    root_cause = "unknown"

    # -----------------------------
    # FAISS SIMILARITY SEARCH
    # -----------------------------
    similar_alarms = faiss_store.search(alarm, k=3)

    filtered_similar = [
        a for a in similar_alarms
        if a.get("score", 0) >= SIMILARITY_THRESHOLD
    ]

    similar_cases_found = len(filtered_similar)

    # ✅ Deduplicate root causes
    similar_root_causes = list(set([
        a.get("root_cause", "unknown") for a in filtered_similar
    ]))

    similarity_score = 0

    # -----------------------------
    # CLASSIFICATION (RULES)
    # -----------------------------
    if normalized_type == "resource_issue":
        if "memory" in text:
            root_cause = "Memory leak causing sustained heap growth"
        elif "cpu" in text:
            root_cause = "CPU saturation due to process spike"
        else:
            root_cause = "Resource utilization issue"

        category = "performance_degradation"
        confidence = 0.9

    elif any(k in text for k in ["link down", "interface down"]):
        category = "link_failure"
        root_cause = "Physical link failure (fiber cut / interface down)"
        confidence = 0.9

    elif "flap" in text:
        category = "flapping_issue"
        root_cause = "Intermittent interface instability"
        confidence = 0.8

    elif any(k in text for k in ["power", "shutdown"]):
        category = "power_failure"
        root_cause = "Power supply interruption"
        confidence = 0.9

    elif any(k in text for k in ["config", "vlan", "route"]):
        category = "configuration_issue"
        root_cause = "Configuration mismatch"
        confidence = 0.85

    elif any(k in text for k in ["hardware", "fan", "temperature"]):
        category = "hardware_failure"
        root_cause = "Hardware component failure"
        confidence = 0.9

    # -----------------------------
    # FAISS IMPROVEMENT LOGIC
    # -----------------------------
    if filtered_similar:
        top_match = max(filtered_similar, key=lambda x: x.get("score", 0))
        

        if top_match.get("root_cause"):
            root_cause = top_match["root_cause"]

        # Dynamic confidence boost
        confidence_boost = min(0.15, similarity_score * 0.15)
        confidence += confidence_boost

    # -----------------------------
    # ANOMALY BOOST
    # -----------------------------
    if is_anomaly:
        confidence += 0.05

    # ✅ Cap confidence (IMPORTANT)
    confidence = min(confidence, 0.95)

    # -----------------------------
    # CORRELATION LOGIC
    # -----------------------------
    is_correlated = False
    correlation_score = 0.0
    correlation_reason = None

    if parent_device:
        is_correlated = True
        correlation_score = 0.85
        correlation_reason = f"Downstream dependency on {parent_device}"

    elif impact_score > 0.7:
        is_correlated = True
        correlation_score = 0.75
        correlation_reason = "High impact across services/customers"

    elif len(neighbors) > 0:
        is_correlated = True
        correlation_score = 0.6
        correlation_reason = "Topology adjacency correlation"

    correlation_id = None
    if is_correlated:
        correlation_id = "CORR-" + str(uuid.uuid4())[:8]

    # -----------------------------
    # ROOT CAUSE PROBABILITY
    # -----------------------------
    severity_weight = {
        "critical": 1.0,
        "major": 0.8,
        "minor": 0.6
    }.get(severity, 0.5)

    root_cause_probability = round(
        (confidence * 0.5) +
        (impact_score * 0.3) +
        (severity_weight * 0.2),
        2
    )

    # ✅ Cap probability
    root_cause_probability = min(root_cause_probability, 0.95)

    # -----------------------------
    # REASONING
    # -----------------------------
    reasoning = (
        f"Alarm '{alarm_type}' detected on {device_name}. "
        f"Normalized type: {normalized_type}. "
        f"FAISS found {similar_cases_found} similar cases. "
        f"Top similarity score: {round(similarity_score, 2)}. "
        f"Root cause inferred as '{root_cause}'. "
        f"Confidence: {confidence}. "
        f"Impact score: {impact_score}, priority: {priority}. "
        f"Correlation score: {correlation_score}. "
        f"Root cause probability: {root_cause_probability}."
    )

    classified_alarm = {
        "alarm_id": alarm.get("alarm_id"),
        "alarm_type": alarm_type,
        "normalized_alarm_type": normalized_type,
        "device_name": device_name,
        "device_type": device_type,
        "location": alarm.get("location"),
        "parent_device": parent_device,
        "child_alarms": neighbors,
        "category": category,
        "severity": severity,
        "priority": priority,
        "category_confidence": confidence,
        "classification_method": "rules + faiss similarity",
        "is_correlated": is_correlated,
        "correlation_id": correlation_id,
        "correlation_score": correlation_score,
        "correlation_reason": correlation_reason,
        "root_cause": root_cause,
        "root_cause_probability": root_cause_probability,
        "similar_cases_found": similar_cases_found,
        "top_similar_root_causes": similar_root_causes,
        "similarity_score": similarity_score,   # ✅ NEW
        "impact_score": impact_score,
        "services_affected": services_affected,
        "customers_affected": customers_affected,
        "is_anomaly": is_anomaly,
        "message": message,
        "createdAt": alarm.get("createdAt"),
        "status": "OPEN",
        "reasoning": reasoning
    }

    # -----------------------------
    # STORE INTO FAISS (DEDUP)
    # -----------------------------
    if not is_duplicate(classified_alarm, filtered_similar):
        faiss_store.add(classified_alarm)
        faiss_store.save()

    return classified_alarm
# -----------------------------
# MAIN PROCESS
# -----------------------------
def run_classification():
    print(f"🚀 Listening to Kafka topic '{INPUT_TOPIC}'...")

    IDLE_TIMEOUT = 10
    last_message_time = time.time()
    count = 0

    with open(OUTPUT_FILE, "a") as f:
        while True:
            records = consumer.poll(timeout_ms=1000)

            if records:
                for tp, msgs in records.items():
                    for msg in msgs:
                        alarm = msg.value
                        try:
                            classified = classify_alarm(alarm)

                            result = alarms_collection.insert_one(classified)
                            classified["_id"] = str(result.inserted_id)

                            f.write(json.dumps(classified) + "\n")

                            producer.send(OUTPUT_TOPIC, classified)
                            producer.flush()

                            count += 1
                            print(f"✅ Processed alarm {classified.get('alarm_id')} ({count})")

                            last_message_time = time.time()

                        except Exception as e:
                            print("❌ Error processing alarm:", e)

            else:
                if time.time() - last_message_time > IDLE_TIMEOUT:
                    print(f"🛑 No messages for {IDLE_TIMEOUT} seconds. Exiting classification agent...")
                    break

    consumer.close()
    print("✅ Classification agent completed successfully")


if __name__ == "__main__":
    run_classification()