from db_connection import connect_db
from datetime import datetime

alarms = connect_db()

alarms.update_one(
    {"alarmId": "ALARM-10001"},
    {
        "$set": {
            "timestamps.acknowledged_at": datetime.utcnow()
        }
    }
)