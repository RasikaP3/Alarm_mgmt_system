import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "classified-alarms"
OUTPUT_FILE = "communication_alerts.json"

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="communication-group"
)

print("📢 Communication Agent Listening to Kafka...")


def write_to_file(content):
    """Append JSON objects into a valid JSON array file"""

    try:
        with open(OUTPUT_FILE, "r") as f:
            data = json.load(f)

    except (FileNotFoundError, json.JSONDecodeError):
        data = []

    # Append dictionary (NOT json.dumps)
    data.append(content)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)

for message in consumer:

    alarm = message.value

    device = alarm.get("device", {})
    classification = alarm.get("classification", {})


    communication_payload = {

        "alarm_id": alarm.get("alarmId"),

        "device_name": device.get("id"),
        "location": device.get("location"),

        "event_type": alarm.get("alarmType"),

        "severity": alarm.get("originalSeverity"),

        "status": alarm.get("status"),

        "classification": {
            "category": classification.get("category"),
            "confidence": classification.get("confidence")
        },

        "services_affected": alarm.get("servicesAffected"),
        "customers_affected": alarm.get("customersAffected"),
        "ip": alarm.get("ip"),
        "source": alarm.get("source"),
        "message": alarm.get("message"),
        "created_at": alarm.get("createdAt")
    }

    print(json.dumps(communication_payload, indent=4))

    write_to_file(communication_payload)

    print("📢 Communication generated\n")