"""
agents/incident_agent.py  — Incident Management Agent (Super Agent)

Responsibilities (per spec):
  ✅ Check if incident already exists (in-memory + MongoDB seed on restart)
  ✅ Group correlated alarms into 1 incident
  ✅ Incident similarity matching — semantic (sentence-transformers + cosine)
       with string-key fallback when model unavailable
  ✅ Decide automation vs human intervention
  ✅ Trigger ticket creation via Kafka topic + handoff file
  ✅ SLA-aware prioritisation
  ✅ Automation confidence scoring
  ✅ Results → result/incident_output.jsonl
               result/ticket_handoff.jsonl

Fixes applied vs original:
  FIX-1  Semantic similarity via SentenceTransformer + cosine — not just string key
  FIX-2  incident_store seeded from MongoDB on startup so restarts don't re-open closed incidents
  FIX-3  sla_breach_forecast added as ISO timestamp (was missing from output)
  FIX-4  alarm_logs preserved and forwarded to ticket handoff
  FIX-5  impacted_services normalised from both field names
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
# OPTIONAL: sentence-transformers (FIX-1)
# Falls back to string-key matching if unavailable
# =========================================
SEMANTIC_OK = False
_model = None
_known_embeddings: list = []   # list of (incident_id, embedding, key)

try:
    from sentence_transformers import SentenceTransformer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    _model = SentenceTransformer("all-MiniLM-L6-v2")
    SEMANTIC_OK = True
    print("✅ Semantic similarity model loaded (all-MiniLM-L6-v2)")
except Exception as e:
    print(f"⚠️ sentence-transformers not available: {e}")
    print("ℹ️  Falling back to string-key similarity matching")


def _embed(text: str):
    """Return embedding vector or None."""
    if not SEMANTIC_OK or _model is None:
        return None
    return _model.encode([text])[0]


def _semantic_match(text: str, threshold: float = 0.82) -> str | None:
    """
    Search known incident embeddings for a cosine match above threshold.
    Returns incident_id of best match, or None.
    """
    if not SEMANTIC_OK or not _known_embeddings:
        return None
    query_vec = _embed(text)
    if query_vec is None:
        return None
    keys     = [e[2] for e in _known_embeddings]
    vecs     = np.array([e[1] for e in _known_embeddings])
    sims     = cosine_similarity([query_vec], vecs)[0]
    best_idx = int(np.argmax(sims))
    if sims[best_idx] >= threshold:
        return _known_embeddings[best_idx][0]   # incident_id
    return None


def _register_embedding(incident_id: str, text: str, key: str):
    vec = _embed(text)
    if vec is not None:
        _known_embeddings.append((incident_id, vec, key))


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
    producer = None

# =========================================
# OPTIONAL: MongoDB  (FIX-2: seed store on startup)
# =========================================
MONGO_OK   = False
collection = None

try:
    from pymongo import MongoClient
    _mc        = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
                             serverSelectionTimeoutMS=3000)
    _mc.server_info()
    collection = _mc["noc"]["incidents"]
    MONGO_OK   = True
    print("✅ Mongo connected")
except Exception as e:
    print(f"⚠️ Mongo not available: {e}")

# =========================================
# IN-MEMORY STORE  (FIX-2: pre-populate from Mongo)
# =========================================
incident_store: dict = {}   # key → incident dict

if MONGO_OK and collection is not None:
    try:
        for doc in collection.find({"status": "open"}, {"_id": 0}):
            # Re-key by correlation_id if present, else similarity key
            restore_key = doc.get("correlation_id") or (
                f"{(doc.get('device') or 'UNKNOWN').upper()}::"
                f"{(doc.get('incident_type') or 'UNKNOWN').upper().replace(' ','_').replace('-','_')}"
            )
            incident_store[restore_key] = doc
            # Re-register embeddings for semantic matching
            embed_text = f"{doc.get('device','')} {doc.get('incident_type','')}"
            _register_embedding(doc["incident_id"], embed_text, restore_key)
        print(f"♻️  Seeded {len(incident_store)} open incidents from MongoDB")
    except Exception as e:
        print(f"⚠️  Could not seed incident store from Mongo: {e}")


# =========================================
# SIMILARITY KEY  (string fallback)
# =========================================
def _similarity_key(alarm: dict) -> str:
    device = (alarm.get("device_name") or "UNKNOWN").upper().strip()
    atype  = (alarm.get("alarm_type")  or "UNKNOWN").upper().strip()
    atype_norm = atype.replace(" ", "_").replace("-", "_")
    return f"{device}::{atype_norm}"


def _find_existing_incident(alarm: dict) -> tuple[str | None, str]:
    """
    Returns (store_key, match_type).
    Priority: explicit correlation_id → semantic embedding → string key.
    """
    corr_id = alarm.get("correlation_id")

    # 1. Explicit correlation
    if corr_id and corr_id in incident_store:
        return corr_id, "correlated_explicit"

    # 2. Semantic match (FIX-1)
    embed_text = (
        f"{alarm.get('device_name','')} "
        f"{alarm.get('alarm_type','')} "
        f"{alarm.get('root_cause','')}"
    )
    sem_id = _semantic_match(embed_text)
    if sem_id:
        # find key by incident_id
        for k, v in incident_store.items():
            if v.get("incident_id") == sem_id:
                return k, "correlated_semantic"

    # 3. String key fallback
    str_key = _similarity_key(alarm)
    if str_key in incident_store:
        return str_key, "correlated_string"

    # 4. New incident — determine key
    new_key = corr_id if corr_id else _similarity_key(alarm)
    return new_key, "new"


# =========================================
# SLA HELPERS
# =========================================
SLA_HOURS = {"P1": 1, "P2": 4, "P3": 12, "P4": 24}

def _priority_from_alarm(alarm: dict) -> str:
    explicit = alarm.get("priority")
    if explicit and explicit in SLA_HOURS:
        return explicit
    return {
        "critical": "P1", "high": "P2", "medium": "P3"
    }.get((alarm.get("severity") or "").lower(), "P4")

def _sla_breach_time(priority: str) -> str:
    hours = SLA_HOURS.get(priority, 24)
    return (datetime.datetime.utcnow() +
            datetime.timedelta(hours=hours)).isoformat()


# =========================================
# AUTOMATION CONFIDENCE SCORING
# =========================================
def _confidence_score(alarm: dict, incident: dict) -> float:
    score = 0.5
    if (alarm.get("severity") or "").lower() == "critical":
        score += 0.3
    if alarm.get("correlation_score", 0) > 0.8:
        score += 0.1
    if alarm.get("impact_score", 0) > 0.7:
        score += 0.1
    if len(incident.get("alarm_ids", [])) > 2:
        score += 0.05
    if SEMANTIC_OK:                                    # semantic match is more reliable
        score += 0.05
    return round(min(score, 1.0), 2)


# =========================================
# CORE PROCESS
# =========================================
def process(alarm: dict) -> dict:
    now    = datetime.datetime.utcnow().isoformat()
    device = alarm.get("device_name", "UNKNOWN")

    key, match_type = _find_existing_incident(alarm)

    # --- BUILD OR UPDATE INCIDENT ---
    if match_type.startswith("correlated") and key in incident_store:
        inc = incident_store[key]
        if alarm["alarm_id"] not in inc["alarm_ids"]:
            inc["alarm_ids"].append(alarm["alarm_id"])
            # Merge alarm logs (FIX-4)
            for log in alarm.get("alarm_logs", []):
                if log not in inc["alarm_logs"]:
                    inc["alarm_logs"].append(log)
        inc["updated_at"] = now
    else:
        inc = {
            "incident_id":   "INC-" + str(uuid.uuid4())[:8].upper(),
            "device":        device,
            "location":      alarm.get("location"),
            "alarm_ids":     [alarm["alarm_id"]],
            "incident_type": alarm.get("normalized_alarm_type") or alarm.get("alarm_type", "UNKNOWN"),
            "status":        "open",
            "created_at":    now,
            "updated_at":    now,
            "alarm_logs":    alarm.get("alarm_logs", []),   # FIX-4
        }
        # Register embedding for future similarity checks (FIX-1)
        embed_text = f"{device} {inc['incident_type']} {alarm.get('root_cause','')}"
        _register_embedding(inc["incident_id"], embed_text, key)

    # --- PRIORITY & SLA ---
    priority         = _priority_from_alarm(alarm)
    sla_breach_time  = _sla_breach_time(priority)          # FIX-3

    # --- CONFIDENCE & ACTION ---
    confidence = _confidence_score(alarm, inc)
    action = (
        "auto_remediation"
        if (confidence > 0.8 and priority != "P1")
        else "create_ticket"
    )

    # --- NORMALISE impacted_services (FIX-5) ---
    impacted = (
        alarm.get("impacted_services")
        or alarm.get("services_affected")
        or []
    )
    # Normalize impacted_services properly
    if impacted in (None, "", [], 0, "0"):
        impacted = []

    elif isinstance(impacted, list):
        impacted = [str(x).strip() for x in impacted if str(x).strip()]

    elif isinstance(impacted, str):
        impacted = [s.strip() for s in impacted.split(",") if s.strip()]

    elif isinstance(impacted, (int, float)):
    # Convert numeric to safe service name OR ignore
        impacted = [f"service_{impacted}"]

    else:
        impacted = []

    # --- UPDATE INCIDENT ---
    inc.update({
        "priority":           priority,
        "severity":           alarm.get("severity"),
        "confidence":         confidence,
        "impact_score":       alarm.get("impact_score"),
        "correlation_score":  alarm.get("correlation_score"),
        "root_cause":         alarm.get("root_cause"),
        "impacted_services":  impacted,                    # FIX-5
        "services_affected":  impacted,                    # keep legacy name too
        "customers_affected": alarm.get("customers_affected"),
        "sla_breach_time":    sla_breach_time,             # FIX-3
        "sla_breach_forecast":sla_breach_time,             # alias used downstream
        "action":             action,
        "match_type":         match_type,
        "similarity_method":  "semantic" if SEMANTIC_OK else "string_key",
    })

    # Handle CLEAR events
    if (alarm.get("status") or "").upper() == "CLEARED":
        inc["status"]     = "resolved"
        inc["cleared_at"] = alarm.get("cleared_at", now)
        # Remove from store so re-open creates a fresh incident
        incident_store.pop(key, None)
    else:
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

        # Result folder
        write_result("incident_output.jsonl", inc)
        if inc["action"] == "create_ticket":
            write_result("ticket_handoff.jsonl", inc)

        # MongoDB
        if MONGO_OK and collection is not None:
            try:
                collection.update_one(
                    {"incident_id": inc["incident_id"]},
                    {"$set": inc},
                    upsert=True
                )
            except Exception as e:
                print(f"⚠️  Mongo write failed: {e}")

        # Kafka
        if KAFKA_OK and producer:
            producer.send(INCIDENT_TOPIC, inc)
            if inc["action"] == "create_ticket":
                producer.send("incident_to_ticket", inc)

        sim = inc.get("similarity_method", "string_key")
        print(
            f"[{inc['match_type'].upper()}][{sim}] "
            f"{inc['incident_id']} | {inc['device']} | "
            f"{inc['priority']} | confidence={inc['confidence']} | "
            f"{inc['action']}"
        )

    if KAFKA_OK and producer:
        producer.flush()

    print("\n✅ INCIDENT AGENT COMPLETED")
    print("📁 Results → result/incident_output.jsonl")
    print("📁 Handoff → result/ticket_handoff.jsonl")


if __name__ == "__main__":
    run()