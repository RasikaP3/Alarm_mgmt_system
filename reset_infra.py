import redis
from kafka.admin import KafkaAdminClient

# Clear Redis
try:
    r = redis.Redis(host='localhost', port=6379)
    r.flushall()
    print("✅ Redis cleared.")
except Exception as e:
    print(f"❌ Redis error: {e}")

# Clear Kafka
try:
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

    topics = [
        "raw-events",
        "structured-alarms",
        "classified-alarms",
        "suppressed-alarms",
        "incidents"
    ]

    admin.delete_topics(topics)
    admin.close()

    print(f"✅ Kafka topics {topics} deleted.")

except Exception as e:
    print(f"❌ Kafka error: {e}")