import json
from pymongo import MongoClient
from pathlib import Path

from common.config import TOPICS, MONGO_URI, DB_NAME
from common.logger import get_logger
from common.queue_manager import get_producer, send_message

logger = get_logger(__name__)

# ---------------- MONGODB ---------------- #

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

devices_col = db["devices"]
topology_col = db["topology"]
services_col = db["services"]
customers_col = db["customers"]
raw_events_col = db["raw_events"]

# ---------------- KAFKA ---------------- #

producer = get_producer()

# ---------------- FILE PATH ---------------- #

BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = BASE_DIR / "data/network_events.json"
OUTPUT_FILE = BASE_DIR / "data/monitoring_agent_output.jsonl"

OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)


# ---------------- HELPER FUNCTIONS ---------------- #

def get_device_metadata(device_name):
    device = devices_col.find_one({"device_id": device_name})
    if not device:
        return {"device_type": None, "location": None, "vendor": None}

    return {
        "device_type": device.get("type"),
        "location": device.get("location"),
        "vendor": device.get("vendor")
    }

def get_topology_neighbors(device_name):
    neighbors = []

    # Get parent
    parent_doc = topology_col.find_one({"destination": device_name})
    parent = parent_doc.get("source") if parent_doc else None

    # Siblings (same parent)
    if parent:
        siblings = topology_col.find({
            "source": parent,
            "destination": {"$ne": device_name}
        })
        neighbors.extend([s["destination"] for s in siblings])

    # Downstream devices
    downstream = topology_col.find({"source": device_name})
    neighbors.extend([d["destination"] for d in downstream])

    return neighbors

def get_parent_device(device_name):
    parent_doc = topology_col.find_one({"destination": device_name})
    return parent_doc.get("source") if parent_doc else None

def get_services(device_name):
    services = services_col.find({"devices": device_name})
    return [svc["service_name"] for svc in services]


def get_services_affected(device_name):
    return services_col.count_documents({"devices": device_name})


def get_customers_affected(device_name):
    services = services_col.find({"devices": device_name})
    service_ids = [svc["service_id"] for svc in services]
    return customers_col.count_documents({"service_id": {"$in": service_ids}})


def get_topology_neighbors(device_name):
    neighbors = []

    parent_doc = topology_col.find_one({"destination": device_name})
    parent = parent_doc.get("source") if parent_doc else None

    if parent:
        siblings = list(topology_col.find({
            "source": parent,
            "destination": {"$ne": device_name}
        }))
        neighbors = [sib["destination"] for sib in siblings]

        downstream = list(topology_col.find({"source": device_name}))
        neighbors += [d["destination"] for d in downstream]

    return neighbors


# ---------------- CORE LOGIC ---------------- #

def process_alarm(event):
    device_name = event.get("device_name")

    metadata = get_device_metadata(device_name)
    services_list = get_services(device_name)
    services = get_services_affected(device_name)
    customers = get_customers_affected(device_name)
    neighbors = get_topology_neighbors(device_name)
    parent_device = get_parent_device(device_name)


    enriched_alarm = {
        "alarm_id": event.get("alarm_id"),
        "alarm_type": event.get("alarm_type"),
        "alarm_key": event.get("alarm_key"),
        "parent_device" : parent_device,
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
        "services": services_list,
        "services_affected": services,
        "customers_affected": customers,
        "topology_neighbors": neighbors
    }

    return enriched_alarm


# ---------------- MAIN FUNCTION ---------------- #

def run_monitoring_from_file():
    logger.info("Starting Monitoring Agent (File Mode)...")

    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    # Load JSON file
    with open(INPUT_FILE, "r") as f:
        events = json.load(f)

    logger.info(f"Loaded {len(events)} events from file")

    # Check if DB already has data (avoid duplicate insert)
    if raw_events_col.count_documents({}) == 0:
        raw_events_col.insert_many(events)
        logger.info("Inserted events into MongoDB (raw_events)")
    else:
        logger.info("Skipping DB insert (already populated)")

    # Process and send to Kafka
    with open(OUTPUT_FILE, "w") as out:
        for event in events:
            enriched_alarm = process_alarm(event)

            # Send to Kafka
            send_message(producer, TOPICS["STRUCTURED"], enriched_alarm)

            # Write to file
            out.write(json.dumps(enriched_alarm) + "\n")

    producer.flush()

    logger.info("Monitoring processing completed")
    logger.info(f"Output written to {OUTPUT_FILE}")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    run_monitoring_from_file()