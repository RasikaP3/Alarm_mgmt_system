def communication_agent(state):

    alarm = state["alarm"]

    if state["decision"] == "SUPPRESS_TOPOLOGY":

        return {
            "type": "topology",
            "device": alarm["device_name"],
            "root_device": state["root_device"],
            "impact_radius": state["impact_radius"],
            "message": f"Suppressed due to upstream root {state['root_device']}."
        }

    elif state["decision"] == "SUPPRESS_FLAPPING":

        return {
            "type": "flapping",
            "device": alarm["device_name"],
            "message": "Flapping detected. Suppressing repeated alarms."
        }

    return None