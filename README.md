# Agentic AI Fault Management System

This project implements an AI-driven NOC Fault Management system.

Agents implemented:
- Alarm Monitoring Agent
- Alarm Classification Agent
- Communication Agent

Tech Stack:
- Python
- Kafka
- LangGraph
- JSON
- Rule Based Classification

Pipeline Flow:

SNMP / Syslog / Telemetry
        ↓
Monitoring Agent
        ↓
Kafka (structured-alarms)
        ↓
Classification Agent
        ↓
Kafka (classified-alarms)
        ↓
Communication Agent