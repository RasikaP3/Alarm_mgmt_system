# common/utils.py

import uuid
from datetime import datetime, timezone


def generate_alarm_id():
    return str(uuid.uuid4())


def get_current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def safe_get(data, key, default=None):
    return data.get(key, default)


def chunk_list(data, size):
    """Split list into chunks"""
    for i in range(0, len(data), size):
        yield data[i:i + size]