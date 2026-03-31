import os
from dotenv import load_dotenv
from pathlib import Path
 
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
 
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "localhost:9092")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INCIDENT_TOPIC  = os.getenv("INCIDENT_TOPIC",  "incident_events")
TICKET_TOPIC    = os.getenv("TICKET_TOPIC",    "ticket_topic")
DISPATCH_TOPIC  = os.getenv("DISPATCH_TOPIC",  "dispatch_topic")
MONGO_URI       = os.getenv("MONGO_URI",       "mongodb://localhost:27017/")
NVIDIA_API_KEY  = os.getenv("NVIDIA_API_KEY",  "")
NOC_LANGUAGE    = os.getenv("NOC_LANGUAGE",    "English")