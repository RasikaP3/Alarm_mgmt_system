import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from pymongo import MongoClient
from pathlib import Path

# MongoDB connection
client = MongoClient("mongodb://localhost:27017")
db = client["noc_alarm_system"]
devices_col = db["devices"]
topology_col = db["topology"]
services_col = db["services"]
customers_col = db["customers"]
raw_events_col = db["raw_events"]

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

RAW_TOPIC = "rawalarm"
STRUCTURED_TOPIC = "structuredalarms"

BASE_DIR = Path(__file__).resolve().parent.parent

OUTPUT_FILE = BASE_DIR / "data/monitoring_agent_output.jsonl"
OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)  # create folder if not exists

def get_device_metadata(device_name):
    device = devices_col.find_one({"device_id": device_name})
    if not device:
        return {"device_type": None, "location": None, "vendor": None}
    return {
        "device_type": device.get("type"),
        "location": device.get("location"),
        "vendor": device.get("vendor")
    }

def get_services_affected(device_name):
    return services_col.count_documents({"devices": device_name})

def get_services(device_name):
    """
    Returns a list of service names that depend on this device.
    """
    services = services_col.find({"devices": device_name})
    return [svc["service_name"] for svc in services]

def get_customers_affected(device_name):
    services = services_col.find({"devices": device_name})
    service_ids = [svc["service_id"] for svc in services]
    return customers_col.count_documents({"service_id": {"$in": service_ids}})


def get_topology_neighbors(device_name):
    """
    Returns a list of neighboring devices (siblings sharing the same parent or downstream devices).
    """
    neighbors = []

    # Find the parent of this device
    parent_doc = topology_col.find_one({"destination": device_name})
    parent = parent_doc.get("source") if parent_doc else None

    if parent:
        # Find all devices under the same parent (siblings)
        siblings = list(topology_col.find({"source": parent, "destination": {"$ne": device_name}}))
        neighbors = [sib["destination"] for sib in siblings]

        # Add downstream devices of current device
        downstream = list(topology_col.find({"source": device_name}))
        neighbors += [d["destination"] for d in downstream]

    return neighbors


def process_alarm(event):
    device_name = event.get("device_name")
    metadata = get_device_metadata(device_name)
    services_list = get_services(device_name)
    services = get_services_affected(device_name)
    customers = get_customers_affected(device_name)
    neighbors = get_topology_neighbors(device_name)

    enriched_alarm = {
        "alarm_id": event.get("alarm_id"),
        "alarm_type": event.get("alarm_type"),
        "alarm_key": event.get("alarm_key"),
        "parent_device": event.get("parent_device"),
        "createdAt": event.get("createdAt"),
        "device_type": metadata.get("device_type"),
        "device_role": event.get("device_role"),
        "device_name": device_name,
        "device_ip": event.get("device_ip"),
        "device_interface": event.get("device_interface"),
        "location": metadata.get("location"),
        "vendor": metadata.get("vendor"),
        "severity": event.get("severity"),
        "source": event.get("source"),
        "message": event.get("message"),
        "services":services_list,
        "services_affected": services,
        "customers_affected": customers,
        "topology_neighbors": neighbors
    }

    return enriched_alarm


def run_monitoring():
    alarms = []

    events = list(raw_events_col.find({}, {"_id": 0}))
    for event in events:
        producer.send(RAW_TOPIC, event)
        alarm = process_alarm(event)
        alarms.append(alarm)
        producer.send(STRUCTURED_TOPIC, alarm)

    producer.flush()

    # Write to JSONL
    with open(OUTPUT_FILE, "w") as f:
        for alarm in alarms:
            f.write(json.dumps(alarm) + "\n")

    print(f"Monitoring agent processed {len(alarms)} alarms")
    print(f"Output written to {OUTPUT_FILE}")
    producer.close()


if __name__ == "__main__":
    run_monitoring()