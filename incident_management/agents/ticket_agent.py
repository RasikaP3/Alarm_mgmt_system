"""
agents/ticket_agent.py  — Ticket Creation Agent

Responsibilities (per spec):
  ✅ Populate CI, impacted services, location, root cause, suggested resolution
  ✅ Attach alarm logs
  ✅ Assign priority dynamically
  ✅ LLM-based ticket summarisation (NVIDIA / fallback)
  ✅ SLA classification
  ✅ Structured template generation
  ✅ Results → result/ticket_output.jsonl
  ✅ File-based fallback when Kafka unavailable (reads result/ticket_handoff.jsonl)
"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

import json, datetime, time, requests
from pathlib import Path
from config.settings import KAFKA_BOOTSTRAP, INCIDENT_TOPIC, NVIDIA_API_KEY
from utils.result_writer import write_result, RESULT_DIR

# =========================================
# CONFIG
# =========================================
SLA_HOURS = {"P1": 1, "P2": 4, "P3": 12, "P4": 24}

ITSM_CATEGORIES = {
    "configuration_issue": "Software / Configuration",
    "performance_issue":   "Performance / Capacity",
    "hardware_failure":    "Hardware / Infrastructure",
    "connectivity_issue":  "Network / Connectivity",
    "network_issue":       "Network / Connectivity",
}

IDLE_TIMEOUT = 60

# =========================================
# KAFKA (SAFE INIT)
# =========================================
KAFKA_OK = False
consumer = producer = None

try:
    from kafka import KafkaConsumer, KafkaProducer

    consumer = KafkaConsumer(
        "incident_to_ticket",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="ticket-agent-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8")
    )
    KAFKA_OK = True
    print(f"✅ Kafka Connected → {KAFKA_BOOTSTRAP}")
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")
    print("ℹ️  Will fall back to reading result/ticket_handoff.jsonl")

# =========================================
# LLM SUMMARY  (NVIDIA → fallback dict)
# =========================================
def generate_llm_summary(incident: dict) -> dict:
    alarm_text = "\n".join(
        f"- {log.get('message', '')}"
        for log in incident.get("alarm_logs", [])[:3]
    )

    fallback = {
        "summary":            f"Issue on {incident.get('device_id') or incident.get('device')}",
        "description":        f"Incident at {incident.get('location')} — {incident.get('incident_type')}",
        "recommended_action": "Check device logs and restart affected services",
    }

    if not NVIDIA_API_KEY:
        return fallback

    prompt = f"""
Create ITSM ticket JSON.
Device: {incident.get('device_id') or incident.get('device')}
Location: {incident.get('location')}
Priority: {incident.get('priority')}
Type: {incident.get('incident_type')}
Impact Score: {incident.get('impact_score')}
Logs: {alarm_text}

Return ONLY valid JSON (no markdown):
{{
  "summary": "...",
  "description": "...",
  "recommended_action": "..."
}}
"""
    try:
        res = requests.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "meta/llama3-70b-instruct",
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.2, "max_tokens": 300},
            timeout=15,
        )
        if res.status_code != 200:
            return fallback
        content = res.json()["choices"][0]["message"]["content"]
        start, end = content.find("{"), content.rfind("}") + 1
        if start == -1 or end == 0:
            return fallback
        return json.loads(content[start:end])
    except Exception as ex:
        print(f"⚠️ LLM error: {ex}")
        return fallback


# =========================================
# TICKET AGENT
# =========================================
class TicketAgent:
    def __init__(self):
        self.counter  = 0
        self.date_str = datetime.datetime.now().strftime("%Y%m%d")

    def generate_ticket_id(self) -> str:
        self.counter += 1
        return f"TKT-{self.date_str}-{self.counter:04d}"

    def derive_ci(self, device) -> str:
        return (device or "UNKNOWN").upper().replace(" ", "_")

    def detect_root_cause(self, incident_type: str) -> str:
        return {
            "configuration_issue": "Configuration drift detected",
            "performance_issue":   "High resource utilisation",
            "hardware_failure":    "Hardware component failure",
            "connectivity_issue":  "Link / interface down",
            "network_issue":       "Network path issue",
        }.get(incident_type or "", "Undetermined root cause")

    def remediation_steps(self, root_cause: str) -> list:
        base = ["Check device logs", "Validate configuration", "Restart affected service"]
        if "hardware" in root_cause.lower():
            base += ["Dispatch field engineer", "Prepare replacement hardware"]
        if "config" in root_cause.lower():
            base += ["Rollback configuration", "Run config audit"]
        return base

    def sla_due_time(self, priority: str) -> str:
        hours = SLA_HOURS.get(priority, 24)
        return (datetime.datetime.now() +
                datetime.timedelta(hours=hours)).isoformat()

    def sla_level(self, priority: str) -> str:
        return {"P1": "Critical", "P2": "High",
                "P3": "Medium",   "P4": "Low"}.get(priority, "Low")

    def build_ticket(self, incident: dict) -> dict:
        device      = incident.get("device_id") or incident.get("device")
        priority    = incident.get("priority", "P4")
        inc_type    = incident.get("incident_type", "")
        root_cause  = self.detect_root_cause(inc_type)
        llm         = generate_llm_summary(incident)

        return {
            "ticket_id":          self.generate_ticket_id(),
            "incident_id":        incident.get("incident_id"),
            "ci":                 self.derive_ci(device),
            "device_name":        device,
            "location":           incident.get("location"),
            "priority":           priority,
            "severity":           incident.get("severity"),
            "category":           ITSM_CATEGORIES.get(inc_type, "General"),
            "impacted_services":  incident.get("services_affected") or
                                  incident.get("impacted_services", []),
            "impact_score":       incident.get("impact_score"),
            "customers_affected": incident.get("customers_affected"),
            "root_cause":         root_cause,
            "resolution_steps":   self.remediation_steps(root_cause),
            "sla_level":          self.sla_level(priority),
            "sla_due_time":       self.sla_due_time(priority),
            "summary":            llm["summary"],
            "description":        llm["description"],
            "recommended_action": llm["recommended_action"],
            "alarm_logs":         incident.get("alarm_logs", []),
            "alarm_ids":          incident.get("alarm_ids", []),
            "confidence_score":   incident.get("confidence"),
            "created_at":         datetime.datetime.now().isoformat(),
        }


# =========================================
# MAIN
# =========================================
def run():
    print("🎫 Ticket Agent started...")
    agent = TicketAgent()

    def handle(incident: dict):
        ticket = agent.build_ticket(incident)
        write_result("ticket_output.jsonl", ticket)
        if KAFKA_OK and producer:
            producer.send("ticket_topic", ticket)
        print(f"  ✅ Ticket Created: {ticket['ticket_id']} | "
              f"{ticket['device_name']} | {ticket['priority']} | "
              f"{ticket['sla_level']}")
        return ticket

    # ── MODE 1: Kafka ──────────────────────────────────────
    if KAFKA_OK and consumer:
        last_msg_time = time.time()
        try:
            while True:
                records = consumer.poll(timeout_ms=1000)
                if records:
                    for _, messages in records.items():
                        for msg in messages:
                            handle(msg.value)
                            last_msg_time = time.time()
                else:
                    if time.time() - last_msg_time > IDLE_TIMEOUT:
                        print("\n🛑 No messages for 15 s. Stopping.")
                        break
        finally:
            consumer.close()
            if producer:
                producer.flush()

    # ── MODE 2: File fallback ──────────────────────────────
    else:
        handoff = RESULT_DIR / "ticket_handoff.jsonl"
        if not handoff.exists():
            print(f"⚠️  No Kafka and no handoff file at {handoff}. "
                  "Run incident_agent first.")
            return
        print(f"📂 Reading from {handoff}")
        with open(handoff, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    handle(json.loads(line))

    print("✅ Ticket Agent stopped.")
    print("📁 Results → result/ticket_output.jsonl")


if __name__ == "__main__":
    run()