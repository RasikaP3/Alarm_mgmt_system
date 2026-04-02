"""
agents/dispatch_agent.py  — Ticket Dispatch Agent

Responsibilities (per spec):
  ✅ Skill matrix matching
  ✅ Geo-location scoring (haversine)
  ✅ Shift schedule awareness
  ✅ Workload balancing (persistent across restarts via MongoDB)
  ✅ Field force webhook trigger for hardware P1/P2
  ✅ Routing optimisation model (composite score)
  ✅ Workforce AI scheduling (shift-aware)
  ✅ Skill matching engine
  ✅ Results → result/dispatch_output.jsonl
  ✅ File-based fallback when Kafka unavailable

"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

import json, datetime, time, random, requests
from math import radians, cos, sin, asin, sqrt
from pathlib import Path
from config.settings import KAFKA_BOOTSTRAP, MONGO_URI
from utils.result_writer import write_result, RESULT_DIR

FIELD_FORCE_URL = os.getenv("FIELD_FORCE_WEBHOOK", "http://localhost:8080/field-force/dispatch")
IDLE_TIMEOUT    = 120

# =========================================
# MONGO
# =========================================
MONGO_OK = False
engineers_col = dispatch_col = None

try:
    from pymongo import MongoClient
    _mc           = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    _mc.server_info()
    _db           = _mc["noc_system"]
    engineers_col = _db["engineers"]
    dispatch_col  = _db["dispatches"]
    MONGO_OK      = True
    print("✅ Mongo connected")
except Exception as e:
    print(f"⚠️ Mongo not available: {e}")

# =========================================
# KAFKA  (FIX-1: earliest offset)
# =========================================
KAFKA_OK = False
consumer = producer = None

try:
    from kafka import KafkaConsumer, KafkaProducer
    consumer = KafkaConsumer(
        "ticket_topic",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",          # FIX-1: was "latest"
        enable_auto_commit=True,
        group_id="dispatch-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8")
    )
    KAFKA_OK = True
    print("✅ Kafka Connected")
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")
    print("ℹ️  Will fall back to reading result/ticket_output.jsonl")

# =========================================
# ENGINEER SEED
# =========================================
ENGINEER_SEED = [
    {"name":"Priya",  "skill":"network",  "shift":"Morning",   "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
    {"name":"Amit",   "skill":"hardware", "shift":"Morning",   "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
    {"name":"Rahul",  "skill":"config",   "shift":"Morning",   "workload":0,"location":"Mumbai","lat":19.0760,"lon":72.8777},
    {"name":"Sneha",  "skill":"config",   "shift":"Afternoon", "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
    {"name":"Kiran",  "skill":"network",  "shift":"Afternoon", "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
    {"name":"Vikram", "skill":"hardware", "shift":"Afternoon", "workload":0,"location":"Delhi", "lat":28.6139,"lon":77.2090},
    {"name":"Arjun",  "skill":"network",  "shift":"Night",     "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
    {"name":"Neha",   "skill":"hardware", "shift":"Night",     "workload":0,"location":"Mumbai","lat":19.0760,"lon":72.8777},
    {"name":"Rohit",  "skill":"config",   "shift":"Night",     "workload":0,"location":"Pune",  "lat":18.5204,"lon":73.8567},
]

LOCATIONS_COORDS = {
    "Pune":   (18.5204, 73.8567),
    "Mumbai": (19.0760, 72.8777),
    "Delhi":  (28.6139, 77.2090),
}

# Seed from Mongo or use defaults
if MONGO_OK and engineers_col is not None:
    ENGINEERS = list(engineers_col.find({}, {"_id": 0}))
    if not ENGINEERS:
        engineers_col.insert_many(ENGINEER_SEED)
        ENGINEERS = ENGINEER_SEED
        print("🌱 Engineers seeded into MongoDB")
    else:
        print(f"♻️  Loaded {len(ENGINEERS)} engineers from MongoDB")
else:
    ENGINEERS = [e.copy() for e in ENGINEER_SEED]
    print("ℹ️  Using in-memory engineer list")


# =========================================
# ROUTING OPTIMISATION UTILITIES
# =========================================
def haversine(lat1, lon1, lat2, lon2) -> float:
    """Distance in km between two lat/lon points."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 6371 * 2 * asin(sqrt(a))

def current_shift() -> str:
    h = datetime.datetime.now().hour
    if  6 <= h < 14: return "Morning"
    if 14 <= h < 22: return "Afternoon"
    return "Night"

def detect_required_skill(ticket: dict) -> str:
    """Map ticket category and root cause to engineer skill bucket."""
    cat = (ticket.get("category") or "").lower()
    rc  = (ticket.get("root_cause") or "").lower()
    if any(k in cat or k in rc for k in ("network", "connectivity", "link", "routing")):
        return "network"
    if any(k in cat or k in rc for k in ("hardware", "component", "power", "facilities")):
        return "hardware"
    if any(k in cat or k in rc for k in ("config", "configuration", "software", "security")):
        return "config"
    return "network"   # safe default

def score_engineer(eng: dict, skill: str, lat: float, lon: float) -> float:
    """
    Composite routing score:
      +2.0  exact skill match
      +1.5  on-shift
      +0.5  proximity bonus  (decays with distance)
      -0.7  per unit of existing workload
    """
    dist = haversine(eng["lat"], eng["lon"], lat, lon)
    return (
        2.0 * (eng["skill"] == skill)
      + 1.5 * (eng["shift"] == current_shift())
      + 0.5 * (1 / (1 + dist))
      - 0.7 * eng.get("workload", 0)
    )

def assign_engineer(ticket: dict) -> dict:
    skill    = detect_required_skill(ticket)
    loc_name = ticket.get("location") if ticket.get("location") in LOCATIONS_COORDS else "Pune"
    lat, lon = LOCATIONS_COORDS[loc_name]

    scored    = [(e, score_engineer(e, skill, lat, lon)) for e in ENGINEERS]
    max_score = max(s for _, s in scored)
    # Break ties randomly to avoid always assigning the same engineer
    top       = [e for e, s in scored if abs(s - max_score) < 1e-6]
    best      = random.choice(top)

    best["workload"] = best.get("workload", 0) + 1

    if MONGO_OK and engineers_col is not None:
        try:
            engineers_col.update_one(
                {"name": best["name"]},
                {"$set": {"workload": best["workload"]}},
                upsert=True
            )
        except Exception as e:
            print(f"⚠️  Engineer workload update failed: {e}")

    return best


# =========================================
# FIELD FORCE TRIGGER
# =========================================
def trigger_field_force(ticket: dict, engineer: dict) -> dict:
    """POST to field-force webhook for hardware P1/P2 tickets."""
    payload = {
        "ticket_id":   ticket.get("ticket_id"),
        "device_name": ticket.get("device_name"),
        "location":    ticket.get("location"),
        "priority":    ticket.get("priority"),
        "engineer":    engineer.get("name"),
        "action":      "FIELD_DISPATCH",
        "timestamp":   datetime.datetime.now().isoformat(),
    }

    # Log to file regardless
    write_result("field_force_dispatch.jsonl", payload)

    # Publish to Kafka field_force_events topic  (FIX-5)
    if KAFKA_OK and producer:
        try:
            producer.send("field_force_events", payload)
        except Exception as e:
            print(f"⚠️  field_force_events publish failed: {e}")

    # Skip live HTTP if dev webhook
    if FIELD_FORCE_URL == "http://localhost:8080/field-force/dispatch":
        print(f"  🚐 Field force logged (dev mode, no HTTP): {payload['ticket_id']}")
        return payload

    try:
        res = requests.post(FIELD_FORCE_URL, json=payload, timeout=5)
        print(f"  🚐 Field force webhook → HTTP {res.status_code}")
    except Exception as e:
        print(f"  ⚠️  Field force webhook unreachable: {e}")

    return payload


# =========================================
# PROCESS ONE TICKET
# =========================================
def handle(ticket: dict) -> dict:
    engineer = assign_engineer(ticket)
    now      = datetime.datetime.now().isoformat()
    shift    = current_shift()

    output = {
        "ticket":        ticket,
        "engineer":      engineer,
        "dispatch_time": now,
        "shift":         shift,
    }

    # Field force for hardware priority tickets
    field_dispatched = False
    if (engineer.get("skill") == "hardware" and
            ticket.get("priority") in ("P1", "P2")):
        ff = trigger_field_force(ticket, engineer)
        output["field_force_dispatch"] = ff
        field_dispatched = True

    write_result("dispatch_output.jsonl", output)

    if KAFKA_OK and producer:
        producer.send("dispatch_topic", output)

    if MONGO_OK and dispatch_col is not None:
        try:
            # FIX-2: upsert — no DuplicateKeyError on reprocessing
            dispatch_col.update_one(
                {"ticket.ticket_id": ticket.get("ticket_id")},
                {"$set": output},
                upsert=True
            )
        except Exception as e:
            print(f"⚠️  Dispatch Mongo write failed: {e}")

    flag = " [FIELD DISPATCHED]" if field_dispatched else ""
    print(
        f"  ✅ {ticket.get('ticket_id')} → {engineer['name']} "
        f"({engineer['skill']}, {engineer['shift']}){flag}"
    )

    return output


# =========================================
# MAIN
# =========================================
def run():
    print("🚐 Dispatch Agent started...")

    # ── MODE 1: Kafka ──────────────────────────────────────
    if KAFKA_OK and consumer:
        last = time.time()
        try:
            while True:
                records = consumer.poll(timeout_ms=1000)
                if records:
                    for _, msgs in records.items():
                        for msg in msgs:
                            handle(msg.value)
                            last = time.time()
                else:
                    if time.time() - last > IDLE_TIMEOUT:
                        # FIX-3: use constant in message
                        print(f"\n🛑 No messages for {IDLE_TIMEOUT}s. Stopping.")
                        break
        finally:
            consumer.close()
            if producer:
                producer.flush()

    # ── MODE 2: File fallback ──────────────────────────────
    else:
        src = RESULT_DIR / "ticket_output.jsonl"
        if not src.exists():
            print(f"⚠️  No Kafka and no {src}. Run ticket_agent first.")
            return
        print(f"📂 Reading from {src}")
        with open(src, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    handle(json.loads(line))

    print("✅ Dispatch Agent stopped.")
    print("📁 Results → result/dispatch_output.jsonl")


if __name__ == "__main__":
    run()