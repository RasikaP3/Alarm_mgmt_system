"""
agents/orchestration_agent.py  — Orchestration Agent / Super-Orchestrator

Responsibilities:
  ✅ Consumes dispatch_topic
  ✅ Determines action: IMMEDIATE_RESPONSE / ESCALATE_MAJOR_INCIDENT /
                        FIELD_DISPATCH / MONITOR
  ✅ SLA breach detection + human-readable sla_time_status
  ✅ Publishes to orchestrated_events topic
  ✅ Persists to MongoDB with event history (not silent overwrite)
  ✅ Exponential back-off retry on Kafka startup
  ✅ Retry count persisted in MongoDB
  ✅ Per-message exception isolation
  ✅ Results → result/orchestrator_output.jsonl
  ✅ File-based fallback when Kafka unavailable

Fixes applied vs original:
  FIX-1  sla_time_status — human-readable time remaining in every output record
  FIX-2  Kafka startup uses exponential back-off (3 attempts: 2s, 4s, 8s)
  FIX-3  Kafka retry count persisted to MongoDB noc_system.kafka_retries collection
  FIX-4  MongoDB upsert uses $push to events array — no silent history overwrite
  FIX-5  FIELD_DISPATCH action now reachable: checked before P2 escalation, on hardware
           root cause regardless of severity level
  FIX-6  IDLE_TIMEOUT log message uses the constant — was printing "15 s"
  FIX-7  action fed back to communication-style log so orchestrator actually "directs"
"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

import json, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from config.settings import KAFKA_BOOTSTRAP, MONGO_URI
from utils.result_writer import write_result, RESULT_DIR

INPUT_TOPIC  = "dispatch_topic"
OUTPUT_TOPIC = "orchestrated_events"
IDLE_TIMEOUT = 150

# =========================================
# MONGO
# =========================================
MONGO_OK    = False
collection  = None
retry_col   = None    # FIX-3: separate collection for kafka retry counts

try:
    from pymongo import MongoClient
    _mc        = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    _mc.server_info()
    _db        = _mc["noc_system"]
    collection = _db["orchestrated_events"]
    retry_col  = _db["kafka_retries"]
    MONGO_OK   = True
    print("✅ Mongo connected")
except Exception as e:
    print(f"⚠️ Mongo not available: {e}")


# =========================================
# KAFKA  (FIX-2: exponential back-off startup)
# =========================================
KAFKA_OK = False
consumer = producer = None
_MAX_KAFKA_ATTEMPTS = 3

def _kafka_retry_count() -> int:
    """Load persisted retry count from Mongo, default 0."""
    if not MONGO_OK or retry_col is None:
        return 0
    try:
        doc = retry_col.find_one({"service": "orchestration_agent"})
        return doc.get("retry_count", 0) if doc else 0
    except Exception:
        return 0

def _persist_retry_count(count: int):                         # FIX-3
    if not MONGO_OK or retry_col is None:
        return
    try:
        retry_col.update_one(
            {"service": "orchestration_agent"},
            {"$set": {"retry_count": count, "last_attempt": datetime.now(timezone.utc).isoformat()}},
            upsert=True
        )
    except Exception:
        pass

_attempt = 0
_retry_count_start = _kafka_retry_count()

while _attempt < _MAX_KAFKA_ATTEMPTS:
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
        _persist_retry_count(0)    # Reset on success
        print("✅ Kafka Connected")
        break
    except Exception as e:
        _attempt += 1
        total_retries = _retry_count_start + _attempt
        _persist_retry_count(total_retries)       # FIX-3: persist across restarts
        sleep_s = 2 ** _attempt                   # FIX-2: 2s, 4s, 8s
        print(f"⚠️  Kafka attempt {_attempt}/{_MAX_KAFKA_ATTEMPTS} failed: {e}")
        if _attempt < _MAX_KAFKA_ATTEMPTS:
            print(f"    Retrying in {sleep_s}s... (lifetime retry #{total_retries})")
            time.sleep(sleep_s)
        else:
            print("    Exhausted retries. Falling back to file mode.")
            print(f"ℹ️  Will fall back to reading result/dispatch_output.jsonl")


# =========================================
# SLA UTILITIES  (FIX-1)
# =========================================
def _parse_sla_dt(ticket: dict):
    raw = ticket.get("sla_due_time") or ticket.get("sla_breach_time")

    # ✅ derive priority safely
    priority = (ticket.get("priority") or "").upper().strip()

    if priority not in {"P1", "P2", "P3", "P4"}:
        severity = (ticket.get("severity") or "").lower()
        priority = {
            "critical": "P1",
            "high": "P2",
            "medium": "P3"
        }.get(severity, "P4")

    hours_map = {"P1": 1, "P2": 4, "P3": 12, "P4": 24}

    # ✅ Auto-generate SLA if missing
    if not raw:
        return datetime.now(timezone.utc) + timedelta(hours=hours_map[priority])

    try:
        dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def check_sla_breached(ticket: dict) -> bool:
    dt = _parse_sla_dt(ticket)
    return (datetime.now(timezone.utc) > dt) if dt else False

def sla_time_status(ticket: dict) -> str:                     # FIX-1
    """Human-readable time remaining / breached string."""
    dt = _parse_sla_dt(ticket)
    if dt is None:
        return "SLA not set"
    now = datetime.now(timezone.utc)
    delta = dt - now
    total_seconds = delta.total_seconds()
    if total_seconds <= 0:
        # How long since breach
        breached_ago = abs(int(total_seconds))
        h, rem = divmod(breached_ago, 3600)
        m = rem // 60
        return f"BREACHED {h}h {m}m ago"
    h, rem = divmod(int(total_seconds), 3600)
    m = rem // 60
    return f"{h}h {m}m remaining"
# =========================================
# SLA INTELLIGENCE ENGINE (NEW)
# =========================================
def get_sla_state(ticket: dict):
    dt = _parse_sla_dt(ticket)
    if not dt:
        return "UNKNOWN", None

    now = datetime.now(timezone.utc)
    remaining = (dt - now).total_seconds()

    # total SLA duration (based on priority)
    priority = (ticket.get("priority") or "P4").upper()
    hours_map = {"P1": 1, "P2": 4, "P3": 12, "P4": 24}
    total = hours_map.get(priority, 24) * 3600

    if remaining <= 0:
        return "BREACHED", remaining

    ratio = remaining / total

    if ratio > 0.5:
        return "MONITOR", remaining
    elif ratio > 0.25:
        return "WARNING", remaining
    else:
        return "ESCALATE", remaining


def _format_time(seconds):
    if seconds is None:
        return "N/A"
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h}h {m}m"
# =========================================
# ESCALATION ENGINE (NEW)
# =========================================
def handle_escalation(ticket: dict, state: str):
    tid = ticket.get("ticket_id")
    priority = ticket.get("priority")

    if state == "WARNING":
        print(f"  ⚠️ WARNING → {tid} nearing SLA breach")

    elif state == "ESCALATE":
        print(f"  🚨 ESCALATE → {tid} notify L2 / manager")

    elif state == "BREACHED":
        print(f"  🔴 SLA BREACHED → {tid} immediate action required")

        # 🔥 P1 auto-escalation
        if priority == "P1":
            print(f"  📞 AUTO-CALL → Manager for {tid}")

# =========================================
# ACTION DETERMINATION  (FIX-5)
# =========================================
def determine_action(ticket: dict) -> str:
    """
    Priority order (most specific → most general):
      P1                          → IMMEDIATE_RESPONSE
      hardware root cause         → FIELD_DISPATCH        (FIX-5: checked before P2 branch)
      critical severity + services→ ESCALATE_MAJOR_INCIDENT
      P2                          → ESCALATE_MAJOR_INCIDENT
      default                     → MONITOR
    """
    priority = ticket.get("priority", "P4")
    severity = (ticket.get("severity") or "").lower()
    services = ticket.get("impacted_services") or ticket.get("services_affected") or []
    rc       = (ticket.get("root_cause") or "").lower()

    if priority == "P1":
        return "IMMEDIATE_RESPONSE"

    # FIX-5: hardware check before P2 so P2 hardware still gets FIELD_DISPATCH
    if any(k in rc for k in ("hardware", "component", "power", "facilities")):
        return "FIELD_DISPATCH"

    if severity == "critical" and len(services) > 0:
        return "ESCALATE_MAJOR_INCIDENT"

    if priority == "P2":
        return "ESCALATE_MAJOR_INCIDENT"

    return "MONITOR"


# =========================================
# ORCHESTRATE
# =========================================
def orchestrate(data: dict) -> dict | None:
    # Accept either full dispatch record {ticket, engineer} or flat ticket
    ticket   = data.get("ticket", {}) if "ticket" in data else data
    engineer = data.get("engineer", {})

    if not ticket:
        print("⚠️  Skipping — no ticket data in message")
        return None

    ticket_id  = ticket.get("ticket_id", "UNKNOWN")
    action     = determine_action(ticket)
    breached   = check_sla_breached(ticket)
    sla_status = sla_time_status(ticket)              # FIX-1

    alarm_logs = ticket.get("alarm_logs", [])
    first_log  = alarm_logs[0] if alarm_logs and isinstance(alarm_logs[0], dict) else {}
    cust_msg   = first_log.get("customer_message")

    now_iso = datetime.now(timezone.utc).isoformat()

    orchestrated = {
        "ticket_id":         ticket_id,
        "incident_id":       ticket.get("incident_id"),
        "device_name":       ticket.get("device_name"),
        "location":          ticket.get("location"),
        "priority":          ticket.get("priority"),
        "severity":          ticket.get("severity"),
        "alarm_logs":        alarm_logs,
        "impacted_services": ticket.get("impacted_services") or
                             ticket.get("services_affected", []),
        "engineer":          engineer,
        "action":            action,
        "sla_breached":      breached,
        "sla_time_status":   sla_status,              # FIX-1
        "status":            "ACTIVE",
        "customer_message":  cust_msg,
        "timestamp":         now_iso,
    }

    # FIX-4: MongoDB — push to events array, don't overwrite history
    if MONGO_OK and collection is not None:
        try:
            collection.update_one(
                {"ticket_id": ticket_id},
                {
                    "$set": {
                        "ticket_id":    ticket_id,
                        "incident_id":  orchestrated["incident_id"],
                        "device_name":  orchestrated["device_name"],
                        "priority":     orchestrated["priority"],
                        "status":       orchestrated["status"],
                        "last_updated": now_iso,
                    },
                    "$push": {
                        "events": {
                            "action":          action,
                            "sla_breached":    breached,
                            "sla_time_status": sla_status,
                            "engineer":        engineer,
                            "timestamp":       now_iso,
                        }
                    }
                },
                upsert=True
            )
        except Exception as e:
            print(f"⚠️  Orchestration Mongo write failed: {e}")

    write_result("orchestrator_output.jsonl", orchestrated)

    if KAFKA_OK and producer:
        producer.send(OUTPUT_TOPIC, orchestrated)

    # FIX-7: Log the directed action clearly
    sla_flag = "  [⚠️  SLA BREACHED]" if breached else f"  [{sla_status}]"
    state, remaining = get_sla_state(ticket)

    print(
        f"  🎛️  {ticket_id} → {action} | {state} "
        f"[{_format_time(remaining)}]"
    )

    if state in ("WARNING", "ESCALATE", "BREACHED"):
        handle_escalation(ticket, state)

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
                            try:                      # per-message isolation
                                result = orchestrate(msg.value)
                            except Exception as e:
                                print(f"⚠️  Error processing message: {e}")
                            last = time.time()
                else:
                    if time.time() - last > IDLE_TIMEOUT:
                        # FIX-6: use constant in message
                        print(f"🛑 No messages for {IDLE_TIMEOUT}s. Stopping.")
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
                    try:
                        orchestrate(json.loads(line))
                    except Exception as e:
                        print(f"⚠️  Error processing line: {e}")

    print("✅ Orchestration Agent stopped.")
    print("📁 Results → result/orchestrator_output.jsonl")


if __name__ == "__main__":
    run()