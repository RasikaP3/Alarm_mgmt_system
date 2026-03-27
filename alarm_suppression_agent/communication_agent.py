def communication_agent(state):

    alarm = state["alarm"]

    if state["decision"] == "SUPPRESS_TOPOLOGY":

        return {
            "type": "topology",
            "device": alarm["device_name"],
            "root_device": state["root_device"],
            "impact_radius": state["impact_radius"],
            "message": f"Root cause {state['root_device']} detected. Impact {state['impact_radius']}. SLA applied."
        }

    elif state["decision"] == "SUPPRESS_FLAPPING":

        return {
            "type": "flapping",
            "device": alarm["device_name"],
            "message": "Flapping detected. Suppressing repeated alarms."
        }

    return None