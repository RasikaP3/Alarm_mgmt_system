import json
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = BASE_DIR / "data" / "monitoring_agent_output.jsonl"

MTTA_OUTPUT = BASE_DIR / "data" / "mtta_results.json"
MTTR_OUTPUT = BASE_DIR / "data" / "mttr_results.json"


def calculate_metrics():

    mtta_results = []
    mttr_results = []

    with open(INPUT_FILE) as f:
        for line in f:
            alarm = json.loads(line)

            alarmId = alarm.get("alarmId")

            lifecycle = alarm.get("lifecycle", {})

            opened = lifecycle.get("openedAt")
            ack = lifecycle.get("acknowledgedAt")
            cleared = lifecycle.get("clearedAt")

            # ---------- MTTA ----------
            if opened and ack:
                opened_dt = datetime.fromisoformat(opened)
                ack_dt = datetime.fromisoformat(ack)

                mtta = (ack_dt - opened_dt).total_seconds()

                mtta_results.append({
                    "alarmId": alarmId,
                    "mtta_seconds": mtta
                })

            # ---------- MTTR ----------
            if opened and cleared:
                opened_dt = datetime.fromisoformat(opened)
                clear_dt = datetime.fromisoformat(cleared)

                mttr = (clear_dt - opened_dt).total_seconds()

                mttr_results.append({
                    "alarmId": alarmId,
                    "mttr_seconds": mttr
                })

    # Save MTTA results
    with open(MTTA_OUTPUT, "w") as f:
        json.dump(mtta_results, f, indent=2)

    # Save MTTR results
    with open(MTTR_OUTPUT, "w") as f:
        json.dump(mttr_results, f, indent=2)

    print("✅ MTTA and MTTR calculated per alarm")


if __name__ == "__main__":
    calculate_metrics()