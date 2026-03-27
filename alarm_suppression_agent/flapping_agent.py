from datetime import timedelta, datetime
from collections import defaultdict

FLAP_WINDOW = timedelta(seconds=120)
FLAP_THRESHOLD = 3

flap_memory = defaultdict(list)
flap_state = {}

def get_ts(alarm):
    return datetime.fromisoformat(alarm["createdAt"].replace("Z",""))

def flapping_agent(state):

    alarm = state["alarm"]
    device = alarm["device_name"]
    ts = get_ts(alarm)

    history = flap_memory[device]
    history = [t for t in history if ts - t < FLAP_WINDOW]

    if alarm["status"] == "OPEN":
        history.append(ts)

    flap_memory[device] = history

    if len(history) >= FLAP_THRESHOLD:
        flap_state[device] = True

    if len(history) == 0:
        flap_state[device] = False

    state["is_flapping"] = flap_state.get(device, False)

    return state