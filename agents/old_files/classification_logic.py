def alarm_classification_agent(alarms):

    for alarm in alarms:

        event_type = alarm.get("event_type", "").lower()
        message = alarm.get("message", "").lower()
        raw = alarm.get("raw_event", {})
        
        if "config" in event_type or "config" in message:
            category = "Configuration Issue"
            confidence = 0.90

        elif "performance" in event_type or "cpu" in message:
            category = "Performance Degradation"
            confidence = 0.88

        elif "link" in event_type:
            category = "Link Failure"
            confidence = 0.92

        elif "power" in message:
            category = "Power Failure"
            confidence = 0.89

        else:
            category = "Hardware Failure"
            confidence = 0.75

        alarm["classification"] = {
            "category": category,
            "confidence": confidence
        }

    return alarms