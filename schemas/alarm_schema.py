from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Device(BaseModel):
    device_id: str
    device_name: str
    device_type: str
    ip_address: str
    location: str


class Event(BaseModel):
    event_type: str
    raw_message: str
    timestamp: datetime


class ServiceImpact(BaseModel):
    impacted_service: str
    customer_impact: str
    sla_breach_risk: bool


class Classification(BaseModel):
    category: str
    confidence_score: float
    reasoning_summary: str


class Alarm(BaseModel):
    alarm_id: str
    source_type: str
    device: Device
    event: Event
    severity: str
    service_impact: ServiceImpact
    classification: Optional[Classification] = None