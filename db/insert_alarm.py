from db_connection import connect_db
from alarm_schema import create_alarm

def insert_alarm(ip, source, message):

    alarms = connect_db()

    alarm_object = create_alarm(ip, source, message)

    result = alarms.insert_one(alarm_object)

    print("Alarm inserted with ID:", result.inserted_id)