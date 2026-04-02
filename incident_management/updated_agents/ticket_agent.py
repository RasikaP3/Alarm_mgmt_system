"""
agents/ticket_agent.py  — Ticket Creation Agent
"""

import sys, os, re
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
    "security_incident":   "Security",
    "power_issue":         "Facilities / Power",
}

ROOT_CAUSE_MAP = {
    "configuration_issue": "Configuration drift detected on device",
    "performance_issue":   "High resource utilisation exceeding threshold",
    "hardware_failure":    "Hardware component failure detected",
    "connectivity_issue":  "Link or interface down — connectivity lost",
    "network_issue":       "Network path issue — routing or switching fault",
    "security_incident":   "Unauthorised access or anomalous traffic detected",
    "power_issue":         "Power supply or UPS fault",
}

REMEDIATION_MAP = {
    "configuration_issue": [
        "Review recent configuration changes",
        "Rollback to last known good configuration",
    ],
    "performance_issue": [
        "Check CPU, memory and interface utilisation",
        "Identify top processes or traffic flows",
    ],
    "hardware_failure": [
        "Dispatch field engineer to site",
        "Prepare replacement hardware",
    ],
    "connectivity_issue": [
        "Check physical cable and port status",
        "Verify interface configuration",
    ],
    "network_issue": [
        "Check routing table",
        "Verify MPLS labels",
    ],
    "security_incident": [
        "Isolate affected device",
        "Capture traffic logs",
    ],
    "power_issue": [
        "Check UPS status",
        "Verify power feed",
    ],
}

IDLE_TIMEOUT = 90

# =========================================
# KAFKA
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
    print("ℹ️ Will fall back to file mode")

# =========================================
# LLM SUMMARY
# =========================================
def _strip_llm_json(content: str) -> str:
    cleaned = re.sub(r"```(?:json)?", "", content, flags=re.IGNORECASE)
    return cleaned.strip()

def generate_llm_summary(incident: dict) -> dict:
    device = incident.get("device_id") or "UNKNOWN"
    inc_type = incident.get("incident_type", "")

    fallback = {
        "summary": f"Issue on {device}",
        "description": f"{inc_type} detected",
        "recommended_action": "Check logs; Restart service",
    }

    if not NVIDIA_API_KEY:
        return fallback

    try:
        res = requests.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}"},
            json={"model": "meta/llama3-70b-instruct",
                  "messages": [{"role": "user", "content": str(incident)}]},
            timeout=10,
        )
        content = res.json()["choices"][0]["message"]["content"]
        cleaned = _strip_llm_json(content)
        parsed = json.loads(cleaned)
        return parsed
    except Exception:
        return fallback

# =========================================
# TICKET AGENT
# =========================================
class TicketAgent:
    def __init__(self):
        self.counter = 0
        self.date_str = datetime.datetime.now().strftime("%Y%m%d")

    def generate_ticket_id(self):
        self.counter += 1
        return f"TKT-{self.date_str}-{self.counter:04d}"

    def normalise_services(self, incident: dict) -> list:
        raw = incident.get("impacted_services") or incident.get("services_affected")

        if raw in (None, "", [], 0, "0"):
            return []

        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]

        if isinstance(raw, str):
            return [s.strip() for s in raw.split(",") if s.strip()]

        if isinstance(raw, (int, float)):
            print(f"⚠️ Invalid impacted_services: {raw}")
            return [f"service_{raw}"]

        return []

    def build_ticket(self, incident):
        llm = generate_llm_summary(incident)

        return {
            "ticket_id": self.generate_ticket_id(),
            "device": incident.get("device_id"),
            "impacted_services": self.normalise_services(incident),
            "summary": llm["summary"],
            "description": llm["description"],
        }

# =========================================
# MAIN
# =========================================
def run():
    print("🎫 Ticket Agent started...")
    agent = TicketAgent()

    def handle(incident):
        try:
            ticket = agent.build_ticket(incident)
        except Exception as e:
            print("❌ Error:", e)
            return

        write_result("ticket_output.jsonl", ticket)

        if KAFKA_OK:
            producer.send("ticket_topic", ticket)

        print("✅ Ticket created:", ticket["ticket_id"])

    if KAFKA_OK:
        for msg in consumer:
            handle(msg.value)

    else:
        handoff = RESULT_DIR / "ticket_handoff.jsonl"
        if handoff.exists():
            with open(handoff) as f:
                for line in f:
                    handle(json.loads(line))

if __name__ == "__main__":
    run()