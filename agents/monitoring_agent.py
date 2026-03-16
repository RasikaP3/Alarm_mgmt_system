import json
import uuid
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from kafka import KafkaProducer
from pymongo import MongoClient

BASE_DIR = Path(__file__).resolve().parent.parent

OUTPUT_FILE = BASE_DIR / "data" / "monitoring_agent_output.jsonl"

RAW_TOPIC = "raw_alarm"
STRUCTURED_TOPIC = "structured_alarms"

# -------------------------
# KAFKA PRODUCER
# -------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -------------------------
# MONGODB CONNECTION
# -------------------------

client = MongoClient("mongodb://localhost:27017")
db = client["noc_alarm_system"]

devices_col = db["devices"]
topology_col = db["topology"]
services_col = db["services"]
customers_col = db["customers"]
raw_events_col = db["raw_events"]

# -------------------------

def now_utc():
    return datetime.now(timezone.utc)


def simulate_ack(open_time):
    delay = random.randint(10, 120)
    return open_time + timedelta(seconds=delay)


def simulate_clear(open_time):
    delay = random.randint(60, 600)
    return open_time + timedelta(seconds=delay)


# -------------------------
# NORMALIZATION
# -------------------------

def normalize_event(event):

    event_type = event.get("event_type", "UNKNOWN")

    return {
        "timestamp": event.get("timestamp"),
        "device": event.get("device_name", "unknown-device"),
        "interface": event.get("interface"),
        "severity": {"original": event.get("severity", "unknown")},

        "event": {
            "type": event_type.upper(),
            "source": event.get("source", "unknown"),
            "message": event.get("message", "")
        }
    }


# -------------------------
# ENRICHMENT FUNCTIONS
# -------------------------

# def get_device_metadata(device_name):

#     device = devices_col.find_one({"device_id": device_name})

#     if not device:
#         return {}

#     return {
#         "id": device.get("device_id"),
#         "name": device.get("hostname"),
#         "ip": device.get("ip"),
#         "location": device.get("location"),
#         "vendor": device.get("vendor")
#     }

def get_device_metadata(device_name):

    device = devices_col.find_one({"device_id": device_name})

    if not device:

        # fallback if device not found
        return {
            "id": device_name,
            "name": device_name,
            "ip": None,
            "location": None,
            "vendor": None
        }

    return {
        "id": device.get("device_id", device_name),
        "name": device.get("hostname", device_name),
        "ip": device.get("ip"),
        "location": device.get("location"),
        "vendor": device.get("vendor")
    }


def get_services_affected(device_name):

    return services_col.count_documents({
        "devices": device_name
    })


def get_customers_affected(device_name):

    services = services_col.find({"devices": device_name})

    service_ids = [svc["service_id"] for svc in services]

    return customers_col.count_documents({
        "service_id": {"$in": service_ids}
    })


def get_parent_device(device_name):

    topo = topology_col.find_one({"destination": device_name})

    if topo:
        return topo.get("source")

    return None


# -------------------------
# PROCESS ALARM
# -------------------------

# def process_alarm(event):

#     event = normalize_event(event)

#     opened_at = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

#     ack_time = simulate_ack(opened_at)
#     clear_time = simulate_clear(opened_at)

#     device_name = event["device"]

#     device_meta = get_device_metadata(device_name)

#     services = get_services_affected(device_name)

#     customers = get_customers_affected(device_name)

#     parent = get_parent_device(device_name)

#     alarm = {

#         "alarmId": str(uuid.uuid4()),

#         "timestamp": event["timestamp"],

#         "device": device_meta,

#         "event": {
#             "type": event["event"]["type"],
#             "source": event["event"]["source"],
#             "message": event["event"]["message"]
#         },

#         "severity": {
#             "original": event["severity"]["original"],
#             "adjusted": event["severity"]["original"]
#         },

#         "status": "OPEN",

#         "dedupCount": 1,

#         "servicesAffected": services,

#         "customersAffected": customers,

#         "impact": {
#             "businessImpact": "LOW",
#             "priority": "P3"
#         },

#         "topology": {
#             "parentDevice": parent
#         },

#         "lifecycle": {
#             "createdAt": opened_at.isoformat(),
#             "acknowledgedAt": "null",
#             "resolvedAt": "null"
#         }
#     }

#     return alarm
# -------------------------
# PROCESS ALARM
# -------------------------
# def process_alarm(event):
#     """
#     Process a raw event into a structured alarm with lifecycle timestamps and status.
#     This includes:
#     - Normalization
#     - Device metadata enrichment
#     - Services/customers affected
#     - Topology info
#     - Lifecycle timestamps: createdAt, acknowledgedAt, resolvedAt
#     - Status flow: OPEN → ACKNOWLEDGED → RESOLVED/CLEARED
#     """

#     # 1️⃣ Normalize incoming raw event
#     event = normalize_event(event)

#     # 2️⃣ Convert raw event timestamp to datetime
#     opened_at = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

#     # 3️⃣ Simulate acknowledgment and clear times (for MTTA/MTTR calculations)
#     ack_time = simulate_ack(opened_at)     # when operator/system acknowledges
#     clear_time = simulate_clear(opened_at) # when alarm is resolved/cleared

#     # 4️⃣ Get device metadata, services, customers, and parent device
#     device_name = event["device"]
#     device_meta = get_device_metadata(device_name)
#     services = get_services_affected(device_name)
#     customers = get_customers_affected(device_name)
#     parent = get_parent_device(device_name)

#     # 5️⃣ Initialize alarm object
#     alarm = {
#         "alarmId": str(uuid.uuid4()),
#         "timestamp": event["timestamp"],
#         "device": device_meta,
#         "event": {
#             "type": event["event"]["type"],
#             "source": event["event"]["source"],
#             "message": event["event"]["message"]
#         },
#         "severity": {
#             "original": event["severity"]["original"],
#             "adjusted": event["severity"]["original"]
#         },
#         "status": "OPEN",  # initial status at creation
#         "dedupCount": 1,
#         "servicesAffected": services,
#         "customersAffected": customers,
#         "impact": {
#             "businessImpact": "LOW",
#             "priority": "P3"
#         },
#         "topology": {
#             "parentDevice": parent
#         },
#         "lifecycle": {
#             "createdAt": opened_at.isoformat(),  # always set at creation
#             "acknowledgedAt": None,              # will be set later
#             "resolvedAt": None                    # will be set later
#         },
#         # 6️⃣ Internal fields to simulate status flow for MTTA/MTTR
#         "_simulated_ack_time": ack_time,
#         "_simulated_clear_time": clear_time
#     }

#     # 7️⃣ Update status dynamically based on simulated times
#     current_time = now_utc()

#     # If current time reached ack_time, mark as ACKNOWLEDGED
#     if current_time >= ack_time and not alarm["lifecycle"]["acknowledgedAt"]:
#         alarm["lifecycle"]["acknowledgedAt"] = ack_time.isoformat()
#         alarm["status"] = "ACKNOWLEDGED"

#     # If current time reached clear_time, mark as RESOLVED or CLEARED
#     if current_time >= clear_time and not alarm["lifecycle"]["resolvedAt"]:
#         alarm["lifecycle"]["resolvedAt"] = clear_time.isoformat()
#         # Randomly choose RESOLVED or CLEARED for simulation purposes
#         alarm["status"] = random.choice(["RESOLVED", "CLEARED"])

#     return alarm
# -------------------------
# HELPER FUNCTIONS
# -------------------------

from datetime import datetime, timedelta, timezone

def now_utc():
    """
    Returns current UTC time as timezone-aware datetime
    """
    return datetime.now(timezone.utc)


def simulate_ack(open_time):
    """
    Simulate acknowledgment time by adding a random delay (10-120 seconds)
    Ensures the returned datetime is UTC offset-aware
    """
    delay = random.randint(10, 120)
    return (open_time + timedelta(seconds=delay)).astimezone(timezone.utc)


def simulate_clear(open_time):
    """
    Simulate clear/resolved time by adding a random delay (60-600 seconds)
    Ensures the returned datetime is UTC offset-aware
    """
    delay = random.randint(60, 600)
    return (open_time + timedelta(seconds=delay)).astimezone(timezone.utc)


# -------------------------
# PROCESS ALARM
# -------------------------

def process_alarm(event):
    """
    Converts a raw event into a structured alarm for NOC pipeline.
    Implements:
    - Status flow: OPEN → ACKNOWLEDGED → RESOLVED/CLEARED
    - Lifecycle timestamps: createdAt, acknowledgedAt, resolvedAt
    - Device metadata, services/customers affected, parent device
    """

    # 1️⃣ Normalize event
    event = normalize_event(event)

    # 2️⃣ Convert event timestamp to UTC offset-aware datetime
    opened_at = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

    # 3️⃣ Simulate acknowledgment and clear/resolved times
    ack_time = simulate_ack(opened_at)     # when operator/system acknowledges
    clear_time = simulate_clear(opened_at) # when alarm is resolved/cleared

    # 4️⃣ Get device metadata, services/customers affected, parent device
    device_name = event["device"]
    device_meta = get_device_metadata(device_name)
    services = get_services_affected(device_name)
    customers = get_customers_affected(device_name)
    parent = get_parent_device(device_name)

    # 5️⃣ Initialize alarm with default status and lifecycle
    alarm = {
        "alarmId": str(uuid.uuid4()),
        "timestamp": event["timestamp"],
        "device": device_meta,
        "event": {
            "type": event["event"]["type"],
            "source": event["event"]["source"],
            "message": event["event"]["message"]
        },
        "severity": {
            "original": event["severity"]["original"],
            "adjusted": event["severity"]["original"]
        },
        "status": "OPEN",  # initial status
        "dedupCount": 1,
        "servicesAffected": services,
        "customersAffected": customers,
        "impact": {
            "businessImpact": "LOW",
            "priority": "P3"
        },
        "topology": {
            "parentDevice": parent
        },
        "lifecycle": {
            "createdAt": opened_at.isoformat(),  # always set at creation
            "acknowledgedAt": None,              # will be updated later
            "resolvedAt": None                    # will be updated later
        },
        # Internal fields to simulate status flow for MTTA/MTTR
        "_simulated_ack_time": ack_time,
        "_simulated_clear_time": clear_time
    }

    # 6️⃣ Update status dynamically based on simulated times
    current_time = now_utc()

    # If current time reached ack_time, mark as ACKNOWLEDGED
    if current_time >= ack_time and not alarm["lifecycle"]["acknowledgedAt"]:
        alarm["lifecycle"]["acknowledgedAt"] = ack_time.isoformat()
        alarm["status"] = "ACKNOWLEDGED"

    # If current time reached clear_time, mark as RESOLVED or CLEARED
    if current_time >= clear_time and not alarm["lifecycle"]["resolvedAt"]:
        alarm["lifecycle"]["resolvedAt"] = clear_time.isoformat()
        # Randomly choose RESOLVED or CLEARED for simulation purposes
        alarm["status"] = random.choice(["RESOLVED", "CLEARED"])

    return alarm
# -------------------------
# MONITORING AGENT
# -------------------------

def run_monitoring():
    alarms = []
    dedup_cache = {}

    # fetch events from MongoDB
    events = list(raw_events_col.find({}, {"_id": 0}))

    for event in events:
        producer.send(RAW_TOPIC, event)

        alarm = process_alarm(event)

        key = (
            alarm["device"]["id"],
            alarm["event"]["type"],
            alarm["severity"]["original"]
        )

        if key in dedup_cache:
            dedup_cache[key]["dedupCount"] += 1
        else:
            dedup_cache[key] = alarm

    alarms = list(dedup_cache.values())

    # Send to Kafka
    for alarm in alarms:
        alarm_to_send = alarm.copy()
        alarm_to_send.pop("_simulated_ack_time", None)
        alarm_to_send.pop("_simulated_clear_time", None)
        producer.send(STRUCTURED_TOPIC, alarm_to_send)

    producer.flush()  # flush after sending all messages

    # Write to JSONL file
    with open(OUTPUT_FILE, "w") as f:
        for alarm in alarms:
            alarm_to_write = alarm.copy()
            alarm_to_write.pop("_simulated_ack_time", None)
            alarm_to_write.pop("_simulated_clear_time", None)
            f.write(json.dumps(alarm_to_write) + "\n")

    print(f"Monitoring agent processed {len(alarms)} alarms")
    print(f"Output file: {OUTPUT_FILE }")
    producer.flush()
    producer.close()
# def run_monitoring():

#     alarms = []
#     dedup_cache = {}

#     # fetch events from MongoDB instead of JSON file
#     events = list(raw_events_col.find({}, {"_id": 0}))

#     for event in events:

#         producer.send(RAW_TOPIC, event)

#         alarm = process_alarm(event)

#         key = (
#             alarm["device"]["id"],
#             alarm["event"]["type"],
#             alarm["severity"]["original"]
#         )

#         if key in dedup_cache:
#             dedup_cache[key]["dedupCount"] += 1
#         else:
#             dedup_cache[key] = alarm

#     alarms = list(dedup_cache.values())

#     for alarm in alarms:
        
#              alarm_to_write = alarm.copy()
#              alarm_to_write.pop("_simulated_ack_time", None)
#              alarm_to_write.pop("_simulated_clear_time", None)
#         f.write(json.dumps(alarm_to_write) + "\n")

#     producer.send(STRUCTURED_TOPIC, alarm)
#     producer.flush()
#     producer.close()

if __name__ == "__main__":
    run_monitoring()