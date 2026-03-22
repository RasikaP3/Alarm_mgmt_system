# #!/bin/bash

echo "🚀 Starting all services..."

cd ~/agentic_noc_Final/kafka_2.13-3.7.0

# -------------------------------
# 1. Start Zookeeper (New Window)
# -------------------------------
echo "🟢 Opening Zookeeper window..."

gnome-terminal --title="Zookeeper" -- bash -c "
echo 'Starting Zookeeper...';
bin/zookeeper-server-start.sh config/zookeeper.properties;
exec bash
"

sleep 5

# -------------------------------
# 2. Start Kafka Broker (New Window)
# -------------------------------
echo "🟢 Opening Kafka Broker window..."

gnome-terminal --title="Kafka Broker" -- bash -c "
echo 'Starting Kafka Broker...';
bin/kafka-server-start.sh config/server.properties;
exec bash
"

sleep 5

-------------------------------
3. Start MongoDB
-------------------------------
echo "🟢 Starting MongoDB..."

sudo systemctl start mongod
sudo systemctl status mongod --no-pager

echo "✅ All services started successfully!"

echo "-----------------------------------------"
echo "🚀 NOC TEST PIPELINE STARTED"
echo "-----------------------------------------"