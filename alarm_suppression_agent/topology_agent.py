def get_device_type(device):

    if device is None:
        return None

    if "CORE" in device:
        return "CORE"
    elif "AGG" in device:
        return "AGG"
    else:
        return "ACC"


def topology_agent(state):

    alarm = state["alarm"]

    parent = alarm.get("parent_alarm")
    is_correlated = alarm.get("is_correlated", False)
    root_prob = alarm.get("root_cause_probability", 0)

    suppressed = False
    root_device = None

    if is_correlated and parent and root_prob >= 0.6:
        suppressed = True
        root_device = parent

    impact = alarm.get("services_affected", 0) + alarm.get("customers_affected", 0)

    state["topology_suppressed"] = suppressed
    state["root_device"] = root_device
    state["impact_radius"] = impact
    state["root_type"] = get_device_type(root_device)

    return state