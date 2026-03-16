from datetime import datetime

def create_alarm(ip, source, message):

    alarm = {
        "alarmId": None,

        "raw_event": {
            "source": source,
            "message": message,
            "ip": ip,
            "timestamp": datetime.utcnow()
        },

        "device": {
            "deviceId": None,
            "hostname": None,
            "vendor": None,
            "location": None
        },

        "normalized_event": {
            "event_type": None,
            "severity": None,
            "category": None
        },

        "classification": {
            "alarm_type": None,
            "probability": None,
            "service_impact": None
        },

        "status": "OPEN",

        "timestamps": {
            "created_at": datetime.utcnow(),
            "acknowledged_at": None,
            "resolved_at": None
        },

        "metrics": {
            "mtta_seconds": None,
            "mttr_seconds": None
        },

        "dedup": {
            "duplicate_count": 1
        }
    }

    return alarm