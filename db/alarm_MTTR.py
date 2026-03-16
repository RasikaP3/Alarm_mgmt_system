from datetime import datetime

alarm = alarm.find_one({"alarmId": "ALARM-10001"})

created = alarm["timestamps"]["created_at"]

resolved = datetime.utcnow()

mttr = (resolved - created).total_seconds()

alarm.update_one(
    {"alarmId": "ALARM-10001"},
    {
        "$set": {
            "status": "RESOLVED",
            "timestamps.resolved_at": resolved,
            "metrics.mttr_seconds": mttr
        }
    }
)