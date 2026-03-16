import json
import uuid
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = BASE_DIR / "data" / "network_events.json"
OUTPUT_FILE = BASE_DIR / "data" / "monitoring_agent_output.jsonl"


def now_utc():
    return datetime.now(timezone.utc)


def simulate_ack(open_time):
    """Simulate realistic acknowledgement delay (10–120 sec)"""
    delay = random.randint(10, 120)
    return open_time + timedelta(seconds=delay)


def simulate_clear(open_time):
    """Simulate resolution delay (1–10 min)"""
    delay = random.randint(60, 600)
    return open_time + timedelta(seconds=delay)


def normalize_event(event):
    """
    Convert generator event format -> monitoring format
    """

    event_type = event.get("event_type", "UNKNOWN")

    return {
        "timestamp": event.get("timestamp"),
        "device": event.get("device_name", "unknown-device"),
        "interface": event.get("interface"),
        "severity": event.get("severity", {"original": "unknown"}),

        "event": {
            "type": event_type.upper(),
            "source": event.get("source", "unknown")
        },

        "metrics": {
            "cpu": event.get("cpu_utilization"),
            "memory": event.get("memory_utilization"),
            "latency": event.get("latency_ms"),
            "packet_loss": event.get("packet_loss_percent")
        }
    }


def process_alarm(event):

    # Normalize event first
    event = normalize_event(event)

    opened_at = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

    ack_time = simulate_ack(opened_at)

    lifecycle = {
        "openedAt": opened_at.isoformat(),
        "acknowledgedAt": ack_time.isoformat(),
        "clearedAt": None
    }

    status = "ACKNOWLEDGED"

    if event["event"]["type"] == "CLEAR":
        clear_time = simulate_clear(opened_at)
        lifecycle["clearedAt"] = clear_time.isoformat()
        status = "CLEARED"

    alarm = {
        "alarmId": str(uuid.uuid4()),
        "timestamp": event["timestamp"],
        "device": event["device"],
        "interface": event.get("interface"),
        "event": event["event"],
        "severity": event.get("severity"),
        "status": status,

        "lifecycle": lifecycle,

        "firstSeen": now_utc().isoformat(),
        "lastSeen": now_utc().isoformat(),

        "dedupCount": 1,

        "servicesAffected": 0,
        "customersAffected": 0,

        "impact": {
            "businessImpact": "LOW",
            "priority": "P3"
        },

        "metrics": event.get("metrics")
    }

    return alarm


def run_monitoring():

    alarms = []

    with open(INPUT_FILE) as f:
        events = json.load(f)

    for event in events:
        alarm = process_alarm(event)
        alarms.append(alarm)

    with open(OUTPUT_FILE, "w") as f:
        for alarm in alarms:
            f.write(json.dumps(alarm) + "\n")

    print(f"Monitoring agent processed {len(alarms)} alarms")
    print(f"Output file: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_monitoring()