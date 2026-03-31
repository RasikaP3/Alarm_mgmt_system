"""
agents/orchestration_agent.py  — Orchestration / Super-Orchestrator

Responsibilities:
  ✅ Consumes dispatch_topic
  ✅ Determines action: IMMEDIATE_RESPONSE / ESCALATE / FIELD_DISPATCH / MONITOR
  ✅ SLA breach detection
  ✅ Publishes to orchestrated_events topic
  ✅ Persists to MongoDB
  ✅ Results → result/orchestrator_output.jsonl
  ✅ File-based fallback when Kafka unavailable
"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

import json, time
from datetime import datetime, timezone
from pathlib import Path
from config.settings import KAFKA_BOOTSTRAP, MONGO_URI
from utils.result_writer import write_result, RESULT_DIR

INPUT_TOPIC  = "dispatch_topic"
OUTPUT_TOPIC = "orchestrated_events"
IDLE_TIMEOUT = 120

# =========================================
# MONGO
# =========================================
MONGO_OK   = False
collection = None

try:
    from pymongo import MongoClient
    _mc        = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    _mc.server_info()
    collection = _mc["noc_system"]["orchestrated_events"]
    MONGO_OK   = True
    print("✅ Mongo connected")
except Exception as e:
    print(f"⚠️ Mongo not available: {e}")

# =========================================
# KAFKA
# =========================================
KAFKA_OK = False
consumer = producer = None

try:
    from kafka import KafkaConsumer, KafkaProducer
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="orchestrator-group"
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8")
    )
    KAFKA_OK = True
    print("✅ Kafka Connected")
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")
    print("ℹ️  Will fall back to reading result/dispatch_output.jsonl")

# =========================================
# INTELLIGENCE
# =========================================
def determine_action(ticket: dict) -> str:
    priority = ticket.get("priority", "P4")
    severity = (ticket.get("severity") or "").lower()
    services = ticket.get("impacted_services") or ticket.get("services_affected") or []
    rc       = (ticket.get("root_cause") or "").lower()

    if priority == "P1":
        return "IMMEDIATE_RESPONSE"
    if severity == "critical" and len(services) > 0:
        return "ESCALATE_MAJOR_INCIDENT"
    if "hardware" in rc:
        return "FIELD_DISPATCH"
    if priority == "P2":
        return "ESCALATE_MAJOR_INCIDENT"
    return "MONITOR"

def check_sla(ticket: dict) -> bool:
    try:
        raw = ticket.get("sla_due_time") or ticket.get("sla_breach_time")
        if not raw:
            return False
        due = datetime.fromisoformat(str(raw))
        now = datetime.now(timezone.utc)
        if due.tzinfo is None:
            due = due.replace(tzinfo=timezone.utc)
        return now > due
    except Exception:
        return False

# =========================================
# ORCHESTRATE
# =========================================
def orchestrate(data: dict) -> dict | None:
    ticket   = data.get("ticket", {}) if "ticket" in data else data
    engineer = data.get("engineer", {})

    if not ticket:
        print("⚠️  Skipping — no ticket data")
        return None

    ticket_id   = ticket.get("ticket_id", "UNKNOWN")
    action      = determine_action(ticket)
    sla_breach  = check_sla(ticket)

    alarm_logs  = ticket.get("alarm_logs", [])
    first_log   = alarm_logs[0] if alarm_logs and isinstance(alarm_logs[0], dict) else {}
    cust_msg    = first_log.get("customer_message")

    orchestrated = {
        "ticket_id":          ticket_id,
        "incident_id":        ticket.get("incident_id"),
        "device_name":        ticket.get("device_name"),
        "location":           ticket.get("location"),
        "priority":           ticket.get("priority"),
        "severity":           ticket.get("severity"),
        "alarm_logs":         alarm_logs,
        "impacted_services":  ticket.get("impacted_services") or
                              ticket.get("services_affected", []),
        "engineer":           engineer,
        "action":             action,
        "sla_breached":       sla_breach,
        "status":             "ACTIVE",
        "customer_message":   cust_msg,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }

    # Mongo
    if MONGO_OK and collection is not None:
        collection.update_one(
            {"ticket_id": ticket_id},
            {"$set": orchestrated},
            upsert=True
        )

    write_result("orchestrator_output.jsonl", orchestrated)

    if KAFKA_OK and producer:
        producer.send(OUTPUT_TOPIC, orchestrated)

    return orchestrated


# =========================================
# MAIN
# =========================================
def run():
    print("🎛️  Orchestration Agent started...")

    # ── MODE 1: Kafka ──────────────────────────────────────
    if KAFKA_OK and consumer:
        last = time.time()
        try:
            while True:
                records = consumer.poll(timeout_ms=1000)
                if records:
                    for _, msgs in records.items():
                        for msg in msgs:
                            try:
                                result = orchestrate(msg.value)
                                if result:
                                    sla_flag = " [SLA BREACHED]" if result["sla_breached"] else ""
                                    print(f"  Orchestrated: {result['ticket_id']} → "
                                          f"{result['action']}{sla_flag}")
                                last = time.time()
                            except Exception as e:
                                print(f"⚠️  Error processing message: {e}")
                else:
                    if time.time() - last > IDLE_TIMEOUT:
                        print("🛑 No messages for 15 s. Stopping.")
                        break
        finally:
            consumer.close()
            if producer:
                producer.flush()

    # ── MODE 2: File fallback ──────────────────────────────
    else:
        src = RESULT_DIR / "dispatch_output.jsonl"
        if not src.exists():
            print(f"⚠️  No Kafka and no {src}. Run dispatch_agent first.")
            return
        print(f"📂 Reading from {src}")
        with open(src, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    result = orchestrate(json.loads(line))
                    if result:
                        sla_flag = " [SLA BREACHED]" if result["sla_breached"] else ""
                        print(f"  Orchestrated: {result['ticket_id']} → "
                              f"{result['action']}{sla_flag}")

    print("✅ Orchestration Agent stopped.")
    print("📁 Results → result/orchestrator_output.jsonl")


if __name__ == "__main__":
    run()