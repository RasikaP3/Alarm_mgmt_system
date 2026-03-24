import random
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
import random

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "data/network_events.json"
OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)
# ----------------------------
# TOPOLOGY
# ----------------------------
TOPOLOGY = {
    "core-1": ["agg-1", "agg-2"],
    "agg-1": ["acc-1", "acc-2"],
    "agg-2": ["acc-3", "acc-4"],
    "acc-1": ["s1", "pc1", "s2"],
    "acc-2": ["s3", "s4", "pc2"],
    "acc-3": ["pc3", "s5", "pc4"],
    "acc-4": ["s6", "pc5", "pc6"]
}

ALL_DEVICES = list(set(
    list(TOPOLOGY.keys()) +
    [d for children in TOPOLOGY.values() for d in children]
))

INTERFACES = ["ge-0/0/1", "ge-0/0/2", "ge-0/0/3"]

# ----------------------------
# HELPERS
# ----------------------------
def now():
    return datetime.now(timezone.utc).isoformat()

def random_ip():
    return f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

# ----------------------------
# DEVICE TIERS
# ----------------------------
def get_device_tier(device):
    if device.startswith("core"):
        return "core"
    if device.startswith("agg"):
        return "agg"
    if device.startswith("acc"):
        return "acc"
    return "edge"

# ----------------------------
# TELEMETRY BASELINES
# ----------------------------
DEVICE_BASELINES = {
    "core": {"cpu": 40, "latency": 50, "packet_loss": 0.2},
    "agg":  {"cpu": 45, "latency": 80, "packet_loss": 0.4},
    "acc":  {"cpu": 50, "latency": 120, "packet_loss": 0.8},
    "edge": {"cpu": 35, "latency": 150, "packet_loss": 1.2}
}

def generate_telemetry_metrics(device):
    tier = get_device_tier(device)
    base = DEVICE_BASELINES[tier]

    return {
        "cpu_utilization": round(random.gauss(base["cpu"] + 45, 5), 2),
        "latency_ms": round(random.gauss(base["latency"] + 150, 30), 1),
        "packet_loss_percent": round(random.gauss(base["packet_loss"] + 2, 0.5), 2)
    }

# ----------------------------
# SNMP DYNAMIC LOGIC
# ----------------------------
def generate_snmp_event(event_type):
    if event_type == "link_failure":
        return {
            "source": "snmp",
            "event_type": "link_down",
            "severity": "critical",
            "snmp_data": {
                "ifName": random.choice(INTERFACES),
                "ifOperStatus": "down",
                "ifInErrors": random.randint(10, 200)
            },
            "message": "Interface down detected via SNMP"
        }

    elif event_type == "hardware_failure":
        return {
            "source": "snmp",
            "event_type": "fan_failure",
            "severity": "major",
            "snmp_data": {
                "fan_id": f"Fan-{random.randint(1, 4)}",
                "fan_status": "failed",
                "temperature": round(random.uniform(70, 95), 1)
            },
            "message": "Fan failure detected via SNMP"
        }

    return None

# ----------------------------
# SYSLOG TEMPLATES
# ----------------------------
SYSLOG_TEMPLATES = {
    "link_failure": "%LINK-3-UPDOWN: Interface {iface}, changed state to DOWN",
    "hardware_failure": "%HARDWARE-2-FAILURE: Fan failure detected",
    "power_failure": "%POWER-1-FAILURE: Power supply failure",
    "config_issue": "%SYS-5-CONFIG_I: Configured from console"
}

# ----------------------------
# EVENT GENERATOR
# ----------------------------
def generate_event():
    device = random.choice(ALL_DEVICES)
    event_type = random.choice(list(SYSLOG_TEMPLATES.keys()) + ["performance_degradation"])
    ts = now()

    event = {
        # ---- Alarm Metadata ----
        "alarm_id": str(uuid.uuid4()),
        "host_name": f"{device}.lab.local",
        "ip": random_ip(),
        "first_occurrence": ts,
        "last_occurrence": ts,
        "node_id": f"NODE-{random.randint(1000,9999)}",
        "device_name": device,
        "device_id": f"DEV-{random.randint(10000,99999)}",
        "status": "OPEN",
        "model_id": f"MODEL-{get_device_tier(device).upper()}",
        "identifier": f"{device}:{event_type}",
        "timestamp": ts,
        "event_type": event_type,
        "source": None,
        "severity": None,
        "message": None,
        "telemetry": None,
        "snmp": None
    }

    # TELEMETRY EVENT
    if event_type == "performance_degradation":
        metrics = generate_telemetry_metrics(device)
        event.update({
            "source": "telemetry",
            "severity": "warning",
            "telemetry": metrics,
            "message": f"High CPU {metrics['cpu_utilization']}%, "
                       f"Latency {metrics['latency_ms']}ms"
        })

    # SNMP EVENTS
    elif event_type in ["link_failure", "hardware_failure"]:
        snmp_event = generate_snmp_event(event_type)
        event.update(snmp_event)

    # SYSLOG EVENTS
    else:
        event.update({
            "source": "syslog",
            "severity": "critical",
            "message": SYSLOG_TEMPLATES[event_type].format(
                iface=random.choice(INTERFACES)
            )
        })

    return event

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    events = [generate_event() for _ in range(500)]
    with open(OUTPUT_FILE, "w") as f:
        json.dump(events, f, indent=2)
    print(json.dumps(events, indent=2))