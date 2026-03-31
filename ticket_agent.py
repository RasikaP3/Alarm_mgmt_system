import json
import datetime
import random
import threading
import time
import requests
import os
from kafka import KafkaProducer, KafkaConsumer
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_FILE = BASE_DIR / "data/ticketagent_output.jsonl"
 

# ===============================
# SET NVIDIA API KEY
# ===============================
NVIDIA_API_KEY = "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"

# ===============================
# NEW: LLM Summary Generator (NVIDIA)
# ===============================
def generate_llm_summary(alarm):
    url = "https://integrate.api.nvidia.com/v1/chat/completions"

    prompt = f"""
    Create ITSM ticket summary and description.

    Device: {alarm['device_name']}
    Location: {alarm['location']}
    Severity: {alarm['severity']}
    Services Impacted: {alarm['services_affected']}
    Issue: {alarm['message']}

    Return STRICT JSON:
    {{
        "summary": "...",
        "description": "..."
    }}
    """

    headers = {
        "Authorization": f"Bearer {NVIDIA_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "meta/llama3-70b-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        content = data["choices"][0]["message"]["content"]

        # 🔥 IMPORTANT: Handle non-JSON responses safely
        try:
            return json.loads(content)
        except:
            return {
                "summary": content[:100],
                "description": content
            }

    except Exception as e:
        print("NVIDIA LLM Error:", e)
        return {
            "summary": "Auto-generated network issue",
            "description": "LLM unavailable, fallback description"
        }


# ===============================
# NEW: ITSM Integration (Mock)
# ===============================
def send_to_itsm(ticket):
    url = "https://httpbin.org/post"

    try:
        response = requests.post(url, json=ticket)
        print(f"Ticket pushed to ITSM (status={response.status_code})")
    except Exception as e:
        print("ITSM Error:", e)


# ===============================
# Kafka Producer Agent
# ===============================
class KafkaAgent:
    def __init__(self, topic):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        self.topic = topic

    def send(self, message):
        self.producer.send(self.topic, value=message)
        print(f"Ticket sent: {message['ticket_id']}")


# ===============================
# Ticket Generator Agent
# ===============================
class TicketAgent:
    def __init__(self):
        self.counter = 0
        self.date_str = datetime.datetime.now().strftime("%Y%m%d")

    def generate_ticket_id(self):
        self.counter += 1
        return f"INC-{self.date_str}-{self.counter:04d}"

    @staticmethod
    def detect_root_cause(message):
        m = message.lower()
        if "config" in m:
            return "Configuration issue detected in network device"
        if "power" in m:
            return "Power supply failure in router"
        if "cpu" in m:
            return "High CPU utilization causing performance degradation"
        return "Network connectivity issue"

    @staticmethod
    def remediation(root_cause):
        if "configuration" in root_cause.lower():
            return ["Check recent configuration changes", "Validate device configuration", "Rollback incorrect configuration"]
        if "power" in root_cause.lower():
            return ["Check PSU status", "Verify power cable", "Replace faulty PSU"]
        return ["Check device health", "Verify interface status", "Analyze device logs"]

    @staticmethod
    def assign_priority(severity, services):
        if severity.lower() == "critical" and services > 50:
            return "P1"
        if severity.lower() == "critical":
            return "P2"
        if severity.lower() == "major":
            return "P3"
        return "P4"

    @staticmethod
    def generate_logs(device):
        events = ["CPU HIGH", "PACKET LOSS", "LATENCY HIGH"]
        logs = []
        for _ in range(3):
            t = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logs.append(f"{t} DEVICE={device} EVENT={random.choice(events)}")
        return logs

    @staticmethod
    def sla_due_time(priority):
        now = datetime.datetime.now()
        if priority == "P1":
            return (now + datetime.timedelta(hours=1)).isoformat()
        if priority == "P2":
            return (now + datetime.timedelta(hours=4)).isoformat()
        if priority == "P3":
            return (now + datetime.timedelta(hours=12)).isoformat()
        return (now + datetime.timedelta(days=1)).isoformat()


# ===============================
# Kafka Consumer
# ===============================
class KafkaConsumerAgent:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="noc_dashboard",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )

    def start(self):
        print(f"Listening for tickets...\n")
        for message in self.consumer:
            ticket = message.value
            self.display_ticket(ticket)

    @staticmethod
    def display_ticket(ticket):
        print("======================================")
        print(f"Ticket ID      : {ticket['ticket_id']}")
        print(f"Summary        : {ticket.get('summary','')}")
        print(f"Device         : {ticket['device_name']}")
        print(f"Priority       : {ticket['priority']}")
        print(f"SLA Due Time   : {ticket['sla_due_time']}")
        print(f"Root Cause     : {ticket['root_cause']}")
        print(f"Resolution     : {', '.join(ticket['resolution'])}")
        print(f"Recent Logs    :")
        for log in ticket.get('logs', []):
            print(f"   {log}")
        print("======================================\n")
      


# ===============================
# Alarm Simulation
# ===============================
def simulate_alarms(producer, ticket_agent):
    devices = ["Router1", "Switch2", "Firewall3", "AP4"]
    severities = ["critical", "major", "minor"]
    messages = ["CPU spike detected", "Power failure detected", "Config mismatch found", "Packet loss on interface"]

    while True:
        alarm = {
            "device_name": random.choice(devices),
            "location": random.choice(["NYC", "LA", "Chicago", "Dallas"]),
            "services_affected": random.randint(1, 100),
            "severity": random.choice(severities),
            "message": random.choice(messages)
        }

        root_cause = ticket_agent.detect_root_cause(alarm["message"])
        priority = ticket_agent.assign_priority(alarm["severity"], alarm["services_affected"])
        due_time = ticket_agent.sla_due_time(priority)

        llm_output = generate_llm_summary(alarm)

        ticket = {
            "ticket_id": ticket_agent.generate_ticket_id(),
            "device_name": alarm["device_name"],
            "location": alarm["location"],
            "ci": alarm["device_name"],
            "impacted_services": alarm["services_affected"],
            "priority": priority,
            "sla_due_time": due_time,
            "message": alarm["message"],
            "root_cause": root_cause,
            "resolution": ticket_agent.remediation(root_cause),
            "logs": ticket_agent.generate_logs(alarm["device_name"]),
            "summary": llm_output.get("summary"),
            "description": llm_output.get("description")
        }

        send_to_itsm(ticket)
        producer.send(ticket)
        time.sleep(2)


# ===============================
# MAIN
# ===============================
if __name__ == "__main__":
    topic = "ticket_topic"
    ticket_agent = TicketAgent()
    producer = KafkaAgent(topic)
    consumer = KafkaConsumerAgent(topic)

    threading.Thread(target=consumer.start, daemon=True).start()
    simulate_alarms(producer, ticket_agent)