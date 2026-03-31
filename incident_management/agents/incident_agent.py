"""
agents/incident_agent.py  — Incident Management Agent (Super Agent)

Responsibilities (per spec):
  ✅ Check if incident already exists
  ✅ Group correlated alarms into 1 incident
  ✅ Incident similarity matching (device + type fuzzy key)
  ✅ Decide automation vs human intervention
  ✅ Trigger ticket creation via Kafka / handoff file
  ✅ SLA-aware prioritisation
  ✅ Automation confidence scoring
  ✅ Results → result/incident_output.jsonl
               result/ticket_handoff.jsonl
"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import json, uuid, datetime
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from config.settings import KAFKA_BOOTSTRAP, INCIDENT_TOPIC
from utils.result_writer import write_result

# =========================================
# ENV
# =========================================
input_env = os.getenv("ALARM_INPUT_FILE")
if not input_env:
    raise ValueError("❌ ALARM_INPUT_FILE not set in .env")
INPUT_FILE = Path(input_env)

print("✅ ENV LOADED")
print("📥 Input:", INPUT_FILE)

# =========================================
# OPTIONAL: Kafka
# =========================================
try:
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode()
    )
    KAFKA_OK = True
    print("✅ Kafka connected")
except Exception as e:
    print("⚠️ Kafka not available:", e)
    KAFKA_OK = False

# =========================================
# OPTIONAL: MongoDB
# =========================================
try:
    from pymongo import MongoClient
    _mc = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
                      serverSelectionTimeoutMS=3000)
    _mc.server_info()
    collection = _mc["noc"]["incidents"]
    MONGO_OK = True
    print("✅ Mongo connected")
except Exception:
    MONGO_OK = False

# =========================================
# IN-MEMORY STORE
# Keys: correlation_id  OR  device_alarm_type
# =========================================
incident_store: dict = {}

# =========================================
# SIMILARITY KEY  (requirement: similarity matching)
# Normalises device+type so slight name variations still correlate
# =========================================
def _similarity_key(alarm: dict) -> str:
    device = (alarm.get("device_name") or "UNKNOWN").upper().strip()
    atype  = (alarm.get("alarm_type")  or "UNKNOWN").upper().strip()
    # strip trailing digits so  ROUTER-01 and ROUTER-02 of same type
    # are kept separate, but "CPU_HIGH" vs "CPU HIGH" merge
    atype_norm = atype.replace(" ", "_").replace("-", "_")
    return f"{device}::{atype_norm}"

# =========================================
# CORE PROCESS
# =========================================
def process(alarm: dict) -> dict:
    now = datetime.datetime.utcnow().isoformat()

    device     = alarm.get("device_name", "UNKNOWN")
    alarm_type = alarm.get("alarm_type",  "UNKNOWN")

    # --- CORRELATION PRIORITY ---
    corr_id = alarm.get("correlation_id")
    if corr_id and corr_id in incident_store:
        key        = corr_id
        match_type = "correlated"
    elif corr_id:
        key        = corr_id
        match_type = "new"
    else:
        # similarity key fallback
        key = _similarity_key(alarm)
        match_type = "correlated" if key in incident_store else "new"

    # --- INCIDENT OBJECT ---
    if match_type == "correlated":
        inc = incident_store[key]
        if alarm["alarm_id"] not in inc["alarm_ids"]:
            inc["alarm_ids"].append(alarm["alarm_id"])
        inc["updated_at"] = now
    else:
        inc = {
            "incident_id":    "INC-" + str(uuid.uuid4())[:8].upper(),
            "device":         device,
            "location":       alarm.get("location"),
            "alarm_ids":      [alarm["alarm_id"]],
            "incident_type":  alarm.get("normalized_alarm_type") or alarm_type,
            "status":         "open",
            "created_at":     now,
            "updated_at":     now,
            # preserve raw logs for downstream agents
            "alarm_logs":     alarm.get("alarm_logs", []),
        }

    # =====================================
    # PRIORITY + SLA
    # =====================================
    priority = alarm.get("priority") or {
        "critical": "P1", "high": "P2", "medium": "P3"
    }.get((alarm.get("severity") or "").lower(), "P4")

    sla_hours = {"P1": 1, "P2": 4, "P3": 12}.get(priority, 24)
    sla_time  = (datetime.datetime.utcnow() +
                 datetime.timedelta(hours=sla_hours)).isoformat()

    # =====================================
    # AUTOMATION CONFIDENCE SCORING
    # =====================================
    confidence = 0.5
    if (alarm.get("severity") or "").lower() == "critical":
        confidence += 0.3
    if alarm.get("correlation_score", 0) > 0.8:
        confidence += 0.1
    if alarm.get("impact_score",      0) > 0.7:
        confidence += 0.1
    if len(inc["alarm_ids"]) > 2:          # multiple alarms reinforce confidence
        confidence += 0.05
    confidence = round(min(confidence, 1.0), 2)

    # =====================================
    # DECISION: automation vs human
    # =====================================
    # P1 always goes to human; high confidence non-P1 → auto
    action = (
        "auto_remediation"
        if (confidence > 0.8 and priority != "P1")
        else "create_ticket"
    )

    # =====================================
    # UPDATE INCIDENT
    # =====================================
    inc.update({
        "priority":          priority,
        "severity":          alarm.get("severity"),
        "confidence":        confidence,
        "impact_score":      alarm.get("impact_score"),
        "correlation_score": alarm.get("correlation_score"),
        "root_cause":        alarm.get("root_cause"),
        "services_affected": alarm.get("services_affected"),
        "customers_affected":alarm.get("customers_affected"),
        "sla_breach_time":   sla_time,
        "action":            action,
        "match_type":        match_type,
    })

    # CLEAR EVENT
    if (alarm.get("status") or "").upper() == "CLEARED":
        inc["status"]     = "resolved"
        inc["cleared_at"] = alarm.get("cleared_at", now)

    incident_store[key] = inc
    return inc


# =========================================
# RUN
# =========================================
def run():
    print("\n🚀 INCIDENT AGENT STARTED\n")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"❌ Input file not found: {INPUT_FILE}")

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    alarms = data if isinstance(data, list) else [data]

    for alarm in alarms:
        inc = process(alarm)

        # ── RESULT FOLDER OUTPUT ──────────────────────────
        write_result("incident_output.jsonl", inc)

        if inc["action"] == "create_ticket":
            write_result("ticket_handoff.jsonl", inc)

        # ── MONGO ─────────────────────────────────────────
        if MONGO_OK:
            collection.update_one(
                {"incident_id": inc["incident_id"]},
                {"$set": inc},
                upsert=True
            )

        # ── KAFKA ─────────────────────────────────────────
        if KAFKA_OK:
            producer.send(INCIDENT_TOPIC, inc)
            if inc["action"] == "create_ticket":
                producer.send("incident_to_ticket", inc)

        print(
            f"[{inc['match_type'].upper()}] {inc['incident_id']} | "
            f"{inc['device']} | {inc['priority']} | "
            f"confidence={inc['confidence']} | {inc['action']}"
        )

    if KAFKA_OK:
        producer.flush()

    print("\n✅ INCIDENT AGENT COMPLETED\n")
    print(f"📁 Results → result/incident_output.jsonl")
    print(f"📁 Handoff → result/ticket_handoff.jsonl")


if __name__ == "__main__":
    run()