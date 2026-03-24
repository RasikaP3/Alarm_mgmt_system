# common/config.py

# ---------------- KAFKA ---------------- #
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = {
    "RAW": "rawalarm",
    "STRUCTURED": "structuredalarms",
    "CLASSIFIED": "alarm.classified",
    "COMMUNICATED": "alarm.communicated"
}

# ---------------- MONGODB ---------------- #
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "noc_alarm_system"

COLLECTIONS = {
    "DEVICES": "devices",
    "TOPOLOGY": "topology",
    "SERVICES": "services",
    "CUSTOMERS": "customers",
    "RAW_EVENTS": "raw_events"
}

# ---------------- FILE PATHS ---------------- #
DATA_DIR = "data"