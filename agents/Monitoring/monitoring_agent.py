import json
from datetime import datetime
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

# ---------------- KAFKA ---------------- #

producer = get_producer()

# ---------------- FILE PATH ---------------- #

BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_FILE = BASE_DIR / "data/network_events.json"
OUTPUT_FILE = BASE_DIR / "data/monitoring_agent_output.jsonl"

OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)

# ---------------- IN-MEMORY CACHE (FOR DEDUP) ---------------- #

alarm_cache = {}

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

def get_parent_device(device_name):
    parent_doc = topology_col.find_one({"destination": device_name})
    return parent_doc.get("source") if parent_doc else None

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

def get_services(device_name):
    services = services_col.find({"devices": device_name})
    return [svc["service_name"] for svc in services]

def get_services_affected(device_name):
    return services_col.count_documents({"devices": device_name})

def get_customers_affected(device_name):
    services = services_col.find({"devices": device_name})
    service_ids = [svc["service_id"] for svc in services]
    return customers_col.count_documents({"service_id": {"$in": service_ids}})

# ---------------- NEW FEATURES ---------------- #

# ✅ Deduplication Logic
def handle_dedup(alarm_key, created_at):
    if alarm_key in alarm_cache:
        alarm_cache[alarm_key]["count"] += 1
        alarm_cache[alarm_key]["last_seen"] = created_at
    else:
        alarm_cache[alarm_key] = {
            "count": 1,
            "first_seen": created_at,
            "last_seen": created_at
        }

    return alarm_cache[alarm_key]

# ✅ Severity Adjustment Logic
def adjust_severity(original_severity, services, customers, device_role):
    impact_score = (services * 0.4) + (customers * 0.4)

    if device_role == "core":
        impact_score += 0.2
    elif device_role == "aggregation":
        impact_score += 0.1

    impact_score = min(impact_score, 1.0)

    if impact_score > 0.75:
        adjusted = "critical"
    elif impact_score > 0.5:
        adjusted = "major"
    else:
        adjusted = original_severity

    return adjusted, round(impact_score, 2)

# ✅ Improved Anomaly Detection
def detect_anomaly(alarm_type):
    # Normalize input for consistency
    alarm_type = (alarm_type or "").lower()

    if alarm_type in ["memory_leak", "high_cpu"]:
        anomaly_score = 0.85   # ✅ improved
    else:
        anomaly_score = 0.3

    return anomaly_score > 0.7, anomaly_score

# ✅ Normalization mapping (NEW)
def normalize_alarm_type(alarm_type):
    mapping = {
        "memory_leak": "resource_issue",
        "high_cpu": "resource_issue",
        "link_down": "network_issue",
        "packet_loss": "network_issue"
    }
    return mapping.get(alarm_type.lower(), "unknown")

# ✅ Priority calculation (NEW)
def calculate_priority(severity, impact_score):
    if severity == "critical" or impact_score > 0.8:
        return "P1"
    elif severity == "major":
        return "P2"
    else:
        return "P3"

# ✅ Historical Fault (mock)
def get_historical_data(device_name):
    return {
        "historical_fault_count": 2,
        "last_occurrence": "2026-03-20T08:00:00"
    }

# ---------------- CORE LOGIC ---------------- #

def process_alarm(event):
    device_name = event.get("device_name")
    alarm_key = event.get("alarm_key")
    created_at = event.get("createdAt")

    metadata = get_device_metadata(device_name)
    services_list = get_services(device_name)
    services = get_services_affected(device_name)
    customers = get_customers_affected(device_name)
    neighbors = get_topology_neighbors(device_name)
    parent_device = get_parent_device(device_name)

    # ✅ Dedup
    dedup_info = handle_dedup(alarm_key, created_at)

    # ❗ FIX: Suppress duplicate alarms
    if dedup_info["count"] > 1:
        logger.info(f"Duplicate alarm suppressed: {alarm_key}")
        return None

    # ✅ Severity Adjustment
    adjusted_severity, impact_score = adjust_severity(
        event.get("severity"),
        services,
        customers,
        event.get("device_role")
    )

    # ✅ Anomaly Detection
    is_anomaly, anomaly_score = detect_anomaly(event.get("alarm_type"))

    # ✅ Normalization
    normalized_type = normalize_alarm_type(event.get("alarm_type"))

    # ✅ Priority
    priority = calculate_priority(adjusted_severity, impact_score)

    # ✅ Historical Data
    history = get_historical_data(device_name)

    enriched_alarm = {
        "alarm_id": event.get("alarm_id"),
        "alarm_type": event.get("alarm_type"),
        "normalized_alarm_type": normalized_type,  
        "alarm_key": alarm_key,

        "device_name": device_name,
        "device_type": metadata.get("device_type"),
        "device_role": event.get("device_role"),
        "device_ip": event.get("device_ip"),
        "location": metadata.get("location"),
        "vendor": metadata.get("vendor"),

        "parent_device": parent_device,
        "topology_neighbors": neighbors,

        "services": services_list,
        "services_affected": services,
        "customers_affected": customers,

        "original_severity": event.get("severity"),
        "adjusted_severity": adjusted_severity,
        "impact_score": impact_score,

        #------------Priority field------------------ (NEW)
        "priority": priority,

        "occurrence_count": dedup_info["count"],
        "first_seen": dedup_info["first_seen"],
        "last_seen": dedup_info["last_seen"],

        "historical_fault_count": history["historical_fault_count"],
        "last_occurrence": history["last_occurrence"],

        "is_anomaly": is_anomaly,
        "anomaly_score": anomaly_score,

        "source": event.get("source"),
        "source_type": event.get("source_type"),

        "message": event.get("message"),
        "createdAt": created_at,

        "processed_at": datetime.utcnow().isoformat(),
        "pipeline_stage": "monitoring_agent"
    }

    return enriched_alarm

# ---------------- MAIN FUNCTION ---------------- #

def run_monitoring_from_file():
    logger.info("Starting Monitoring Agent (File Mode)...")

    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r") as f:
        events = json.load(f)

    logger.info(f"Loaded {len(events)} events from file")

    with open(OUTPUT_FILE, "a") as out:
        for event in events:
            enriched_alarm = process_alarm(event)

            # -------------Skip duplicates-------------
            if not enriched_alarm:
                continue

            send_message(producer, TOPICS["STRUCTURED"], enriched_alarm)

            out.write(json.dumps(enriched_alarm) + "\n")
           # ---------- ensure immediate write-------------
            out.flush()  
    producer.flush()

    logger.info("Monitoring processing completed")
    logger.info(f"Output written to {OUTPUT_FILE}")

# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    run_monitoring_from_file()