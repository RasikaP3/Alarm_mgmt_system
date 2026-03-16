import json
import uuid
from datetime import datetime, timedelta
import random

OUTPUT_FILE = "data/network_events.json"

def now():
    return datetime.utcnow()

def make_event(device, interface, event_type, seconds_offset=0):
    ts = now() + timedelta(seconds=seconds_offset)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "device_name": device,
        "interface": interface,
        "event_type": event_type,
        "severity": "critical",
        "source": "syslog",
        "message": f"{event_type} detected"
    }

events = []

# ---------------------------
# NORMAL EVENTS
# ---------------------------
for i in range(20):

    events.append(
        make_event(
            device=f"acc-{random.randint(1,4)}",
            interface="ge-0/0/1",
            event_type="link_failure"
        )
    )

# ---------------------------
# FLAPPING EVENTS
# ---------------------------

for i in range(5):

    events.append(make_event("acc-2","ge-0/0/1","link_failure",i))
    events.append(make_event("acc-2","ge-0/0/1","link_recovery",i+1))
    events.append(make_event("acc-2","ge-0/0/1","link_failure",i+2))
    events.append(make_event("acc-2","ge-0/0/1","link_recovery",i+3))

# ---------------------------
# TOPOLOGY EVENTS
# ---------------------------

events.append(make_event("core-1","ge-0/0/1","hardware_failure"))
events.append(make_event("agg-1","ge-0/0/1","link_failure"))
events.append(make_event("acc-1","ge-0/0/1","link_failure"))
events.append(make_event("s1","ge-0/0/1","link_failure"))

# ---------------------------
# TELEMETRY EVENTS
# ---------------------------

for i in range(10):

    events.append({
        "event_id": str(uuid.uuid4()),
        "timestamp": now().isoformat(),
        "device_name": "acc-3",
        "interface": "system",
        "event_type": "performance_degradation",
        "source": "telemetry",
        "cpu_utilization": random.randint(85,95),
        "latency_ms": random.randint(200,300),
        "packet_loss_percent": random.uniform(3,6),
        "severity": "warning",
        "message": "High CPU and latency"
    })

# ---------------------------
# SAVE
# ---------------------------

with open(OUTPUT_FILE,"w") as f:
    json.dump(events,f,indent=2)

print("✅ Test dataset created")