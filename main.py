from agents.monitoring_agent_old import alarm_monitoring_agent


def main():
    file_path = "data/network_events.json"

    # This will:
    # 1. Ingest
    # 2. Normalize
    # 3. Enrich
    # 4. Publish to Kafka (structured-alarms topic)
    alarm_monitoring_agent(file_path)

    print("✅ Monitoring Agent finished publishing alarms to Kafka")


if __name__ == "__main__":
    main()