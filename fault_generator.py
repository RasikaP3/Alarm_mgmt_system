import json
import uuid
from datetime import datetime
from pathlib import Path
import random

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "data/network_events.json"
OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)

# Topology relationships
TOPOLOGY = {
    "core-1": ["agg-1", "agg-2"],
    "agg-1": ["acc-1", "acc-2"],
    "agg-2": ["acc-3", "acc-4"]
}

ALARM_TYPES = ["LINK_DOWN", "HIGH_CPU", "MEMORY_LEAK", "PORT_FLAP"]

DEVICE_ROLES = {
    "core": "core",
    "agg": "aggregation",
    "acc": "access"
}

def get_device_role(device_name):
    if "core" in device_name:
        return DEVICE_ROLES["core"]
    elif "agg" in device_name:
        return DEVICE_ROLES["agg"]
    else:
        return DEVICE_ROLES["acc"]

def generate_event(device=None, suppressed=False):
    if not device:
        device = random.choice(list(TOPOLOGY.keys()) + sum(TOPOLOGY.values(), []))
    alarm_type = random.choice(ALARM_TYPES)
    now = datetime.utcnow()
    event = {
        "alarm_id": str(uuid.uuid4()),
        "device_name": device,
        "alarm_type": alarm_type,
        "alarm_key": f"{device}_{alarm_type.lower()}",
        "createdAt": now.isoformat(),
        "device_interface": f"eth{random.randint(0, 5)}",
        "device_role": get_device_role(device),
        "device_ip": f"10.0.0.{random.randint(1, 254)}",
        "severity": random.choice(["critical", "major", "minor"]),
        "source": "monitoring_system",
        "message": f"{alarm_type} detected on {device}",
        "suppressed_by_topology": suppressed
    }
    return event

def generate_events(count=50, topology_suppression=False):
    events = []
    for _ in range(count):
        # Decide whether to simulate topology suppression
        suppressed = False
        device = random.choice(list(TOPOLOGY.keys()) + sum(TOPOLOGY.values(), []))

        if topology_suppression and "core" in device:
            # 50% chance to suppress downstream devices
            if random.choice([True, False]):
                suppressed = True
                # generate events for child devices with suppression
                for agg in TOPOLOGY[device]:
                    for acc in TOPOLOGY.get(agg, []):
                        events.append(generate_event(acc, suppressed=True))

        events.append(generate_event(device, suppressed))
    return events

if __name__ == "__main__":
    events = generate_events(count=50, topology_suppression=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(events, f, indent=2)
    print(f"Generated {len(events)} events at {OUTPUT_FILE}")