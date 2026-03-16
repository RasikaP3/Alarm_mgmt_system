import json
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = BASE_DIR / "data" / "monitoring_agent_output.jsonl"
OUTPUT_FILE = BASE_DIR / "data" / "classified_alarms_output.jsonl"

# -----------------------------
# CLASSIFICATION FUNCTION
# -----------------------------
# def classify_alarm(alarm):
#     # Safe extraction
#     event_type = alarm.get("event_type", "").lower()
#     message = alarm.get("message", "").lower()

#     category = "Unknown"
#     confidence = 0.5

#     # Classification rules
#     if "link" in event_type or "link" in message:
#         category = "Link failure"
#         confidence = 0.9
#     elif "hardware" in event_type or "hw" in message:
#         category = "Hardware failure"
#         confidence = 0.9
#     elif "config" in event_type or "configuration" in message:
#         category = "Configuration issue"
#         confidence = 0.85
#     elif "power" in event_type or "power" in message:
#         category = "Power failure"
#         confidence = 0.9
#     elif "performance" in event_type or "degradation" in message or "cpu" in event_type or "memory" in event_type:
#         category = "Performance degradation"
#         confidence = 0.8
#     elif "false" in event_type or "false" in message:
#         category = "False positive"
#         confidence = 0.95

#     # Add classification info to alarm
#     classification = {
#         "classificationId": str(uuid.uuid4()),
#         "classification": {
#             "category": category,
#             "confidence": confidence
#         }
#     }

#     alarm.update(classification)
#     return alarm


def classify_alarm(alarm):
    # Extract nested event fields safely
    event_type = alarm.get("event", {}).get("type", "").lower()
    message = alarm.get("event", {}).get("message", "").lower()

    category = "Unknown"
    confidence = 0.5

    # Classification rules
    if "link" in event_type or "link" in message:
        category = "Link failure"
        confidence = 0.9
    elif "hardware" in event_type or "hw" in message:
        category = "Hardware failure"
        confidence = 0.9
    elif "config" in event_type or "configuration" in message:
        category = "Configuration issue"
        confidence = 0.85
    elif "power" in event_type or "power" in message:
        category = "Power failure"
        confidence = 0.9
    elif "performance" in event_type or "degradation" in message or "cpu" in event_type or "memory" in event_type:
        category = "Performance degradation"
        confidence = 0.8
    elif "false" in event_type or "false" in message:
        category = "False positive"
        confidence = 0.95

    # Add classification info to alarm
    classification = {
        "classificationId": str(uuid.uuid4()),
        "classification": {
            "category": category,
            "confidence": confidence
        }
    }

    alarm.update(classification)
    return alarm
# -----------------------------
# MAIN PROCESS
# -----------------------------
def run_classification():
    alarms = []

    # Read input file
    with open(INPUT_FILE) as f:
        for line in f:
            alarm = json.loads(line)
            classified = classify_alarm(alarm)
            alarms.append(classified)

    # Write output file
    with open(OUTPUT_FILE, "w") as f:
        for alarm in alarms:
            f.write(json.dumps(alarm) + "\n")

    print(f"Classification agent processed {len(alarms)} alarms")
    print(f"Output file: {OUTPUT_FILE}")

if __name__ == "__main__":
    run_classification()