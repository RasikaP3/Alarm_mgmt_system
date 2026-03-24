# common/queue_manager.py

import json
from kafka import KafkaProducer, KafkaConsumer
from common.config import KAFKA_BOOTSTRAP_SERVERS
from common.logger import get_logger

logger = get_logger(__name__)


# ---------------- PRODUCER ---------------- #

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def send_message(producer, topic, message):
    try:
        producer.send(topic, message)
        logger.info(f"Message sent to topic: {topic}")
    except Exception as e:
        logger.error(f"Error sending message: {e}")


# ---------------- CONSUMER ---------------- #

def get_consumer(topic, group_id="default-group"):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )