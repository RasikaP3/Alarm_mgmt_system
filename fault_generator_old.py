import json
import uuid
from datetime import datetime, timedelta
import random

OUTPUT_FILE = "data/network_events.json"

# -------------------------
# NETWORK TOPOLOGY
# -------------------------

TOPOLOGY = {
    "core-1": ["agg-1", "agg-2"],
    "agg-1": ["acc-1", "acc-2"],
    "agg-2": ["acc-3", "acc-4"],
    "acc-1": ["s1", "pc1", "s2"],
    "acc-2": ["s3", "s4", "pc2"],
    "acc-3": ["pc3", "s5", "pc4"],
    "acc-4": ["s6", "pc5", "pc6"]
}

DEVICE_IPS = {
    "core-1": "10.0.0.1",
    "agg-1": "10.0.1.1",
    "agg-2": "10.0.1.2",
    "acc-1": "10.0.2.1",
    "acc-2": "10.0.2.2",
    "acc-3": "10.0.2.3",
    "acc-4": "10.0.2.4",
    "s1": "10.0.3.1",
    "s2": "10.0.3.2",
    "s3": "10.0.3.3",
    "s4": "10.0.3.4",
    "s5": "10.0.3.5",
    "s6": "10.0.3.6",
    "pc1": "10.0.4.1",
    "pc2": "10.0.4.2",
    "pc3": "10.0.4.3",
    "pc4": "10.0.4.4",
    "pc5": "10.0.4.5",
    "pc6": "10.0.4.6"
}

DEVICE_ROLE = {
    "core": "core",
    "agg": "aggregation",
    "acc": "access",
    "s": "switch",
    "pc": "endpoint"
}

def get_role(device):
    for key in DEVICE_ROLE:
        if device.startswith(key):
            return DEVICE_ROLE[key]
    return "unknown"

def get_parent(device):

    for parent, children in TOPOLOGY.items():
        if device in children:
            return parent

    return None


def now():
    return datetime.utcnow()


def make_event(device, interface, event_type, seconds_offset=0):

    ts = now() + timedelta(seconds=seconds_offset)

    parent = get_parent(device)

    return {
        "event_id": str(uuid.uuid4()),
        "alarm_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),

        "device_name": device,
        "ip": DEVICE_IPS.get(device, "0.0.0.0"),
        "interface": interface,

        "event_type": event_type,
        "severity": "critical",
        "source": "syslog",

        "message": f"{event_type} detected",

        "device_role": get_role(device),
        "parent_device": parent,

        "alarm_key": f"{device}_{interface}_link",

        "status": "OPEN",

        "incident_required": True
    }

events = []

# -------------------------
# NORMAL EVENTS
# -------------------------

for i in range(20):

    events.append(
        make_event(
            device=f"acc-{random.randint(1,4)}",
            interface="ge-0/0/1",
            event_type="link_failure"
        )
    )

# -------------------------
# FLAPPING EVENTS
# -------------------------

for i in range(5):

    events.append(make_event("acc-2","ge-0/0/1","link_failure",i))
    events.append(make_event("acc-2","ge-0/0/1","link_recovery",i+1))
    events.append(make_event("acc-2","ge-0/0/1","link_failure",i+2))
    events.append(make_event("acc-2","ge-0/0/1","link_recovery",i+3))

# -------------------------
# TOPOLOGY CASCADE
# -------------------------

events.append(make_event("core-1","ge-0/0/1","hardware_failure"))

events.append(make_event("agg-1","ge-0/0/1","link_failure"))

events.append(make_event("acc-1","ge-0/0/1","link_failure"))

events.append(make_event("s1","ge-0/0/1","link_failure"))

# -------------------------
# TELEMETRY EVENTS
# -------------------------

for i in range(10):

    events.append({

        "event_id": str(uuid.uuid4()),
        "alarm_id": str(uuid.uuid4()),
        "timestamp": now().isoformat(),

        "device_name": "acc-3",
        "ip": DEVICE_IPS.get("acc-3"),

        "interface": "system",
        "event_type": "performance_degradation",

        "cpu_utilization": random.randint(85,95),
        "latency_ms": random.randint(200,300),
        "packet_loss_percent": round(random.uniform(3,6),2),

        "severity": "warning",
        "source": "telemetry",

        "message": "High CPU and latency",

        "device_role": "access",
        "parent_device": "agg-2",

        "alarm_key": "acc-3_system_perf",

        "status": "OPEN",

        "incident_required": False
    })

with open(OUTPUT_FILE,"w") as f:
    json.dump(events,f,indent=2)

print("✅ Test dataset created")