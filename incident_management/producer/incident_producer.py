import json
import time
from kafka import KafkaProducer

# =========================================
# KAFKA SETUP
# =========================================
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    retries=3
)

# ✅ USE CORRECT TOPIC
TOPIC = "validated_alarms"

# =========================================
# INPUT FILE
# =========================================
file_path = r"D:\noc_ai_project\data\communication_alerts 1.json"

try:
    with open(file_path, "r", encoding="utf-8") as f:
        alarms = json.load(f)

    # Handle single object
    if isinstance(alarms, dict):
        alarms = [alarms]

except Exception as e:
    print("❌ File Error:", e)
    exit()

print("🚀 Sending alarms to Kafka...\n")

# =========================================
# PRODUCE EVENTS
# =========================================
for i, alarm in enumerate(alarms, start=1):

    # ✅ Normalize structure (VERY IMPORTANT)
    formatted_alarm = {
        "alarm_id": alarm.get("alarm_id", f"ALARM-{i}"),
        "device_id": alarm.get("device_id", alarm.get("device_name", "UNKNOWN")),
        "location": alarm.get("location", "UNKNOWN"),
        "severity": alarm.get("severity", "medium"),
        "alarm_type": alarm.get("alarm_type", "generic"),
        "message": alarm.get("message", "No message provided")
    }

    try:
        producer.send(TOPIC, formatted_alarm)
        print(f"✅ Sent: {formatted_alarm['device_id']} | {formatted_alarm['message']}")
    except Exception as e:
        print("❌ Kafka Error:", e)

    time.sleep(1)

# ✅ IMPORTANT
producer.flush()

print("\n🎯 All alarms sent successfully!")