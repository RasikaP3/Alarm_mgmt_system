#!/bin/bash

# ---------------------------------
# ⚙️ CONFIGURATION
# ---------------------------------

BASE_DIR=$(pwd)

KAFKA_HOME="$BASE_DIR/kafka_2.13-3.7.0"

LOG_DIR="$BASE_DIR/noc_logs"
mkdir -p "$LOG_DIR"

TOPICS=(
"structuredalarms"
"classifiedalarms"
)

echo "---------------------------------"
echo "🚀 NOC Infrastructure Automator"
echo "---------------------------------"

# ---------------------------------
# 🧹 RESET MODE
# ---------------------------------

if [[ "$1" == "--reset" ]]; then

    echo "⚠️ Reset mode initiated..."

    # Clear Redis
    if command -v redis-cli &> /dev/null; then
        echo "🧹 Clearing Redis..."
        redis-cli FLUSHALL > /dev/null 2>&1
    fi

    # Delete Kafka topics
    echo "🗑️ Deleting Kafka topics..."

    for topic in "${TOPICS[@]}"; do
        $KAFKA_HOME/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --delete \
        --topic "$topic" > /dev/null 2>&1
    done

    # Clear local logs
    if [ -f "classifiedalarms_output.jsonl" ]; then
        echo "📄 Clearing local JSONL logs..."
        > classifiedalarms_output.jsonl
    fi

    echo "✅ Reset complete."

fi

# ---------------------------------
# SERVICE CHECK FUNCTION
# ---------------------------------

check_and_start() {

    local service_name=$1
    local start_cmd=$2

    if pgrep -x "$service_name" > /dev/null || pgrep -f "$service_name" > /dev/null
    then
        echo "🟢 $service_name already running."
    else
        echo "🟡 Starting $service_name..."
        eval "$start_cmd"
        sleep 5
    fi
}

# ---------------------------------
# Start MongoDB
# ---------------------------------

check_and_start "mongod" \
"mongod --fork --logpath $LOG_DIR/mongodb.log"

# ---------------------------------
# Start Redis
# ---------------------------------

check_and_start "redis-server" \
"redis-server --daemonize yes"

# ---------------------------------
# Start Zookeeper
# ---------------------------------

check_and_start "QuorumPeerMain" \
"nohup $KAFKA_HOME/bin/zookeeper-server-start.sh \
$KAFKA_HOME/config/zookeeper.properties \
> $LOG_DIR/zookeeper.log 2>&1 &"

echo "⏳ Waiting for Zookeeper to stabilize..."
sleep 10

# ---------------------------------
# Start Kafka Broker
# ---------------------------------

check_and_start "kafka.Kafka" \
"nohup $KAFKA_HOME/bin/kafka-server-start.sh \
$KAFKA_HOME/config/server.properties \
> $LOG_DIR/kafka.log 2>&1 &"

# ---------------------------------
# Wait for Kafka port
# ---------------------------------

echo "⏳ Waiting for Kafka broker..."

until nc -z localhost 9092
do
    sleep 2
done

echo "🟢 Kafka broker ready."

# ---------------------------------
# Create Topics
# ---------------------------------

echo "🛰️ Verifying Kafka topics..."

for topic in "${TOPICS[@]}"
do

    exists=$($KAFKA_HOME/bin/kafka-topics.sh \
    --list \
    --bootstrap-server localhost:9092 | grep -w "$topic")

    if [ -z "$exists" ]
    then

        echo "🆕 Creating topic: $topic"

        $KAFKA_HOME/bin/kafka-topics.sh \
        --create \
        --topic "$topic" \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1

    else

        echo "🟢 Topic already exists: $topic"

    fi

done

# ---------------------------------
# FINAL STATUS
# ---------------------------------

echo "---------------------------------"
echo "✅ NOC INFRASTRUCTURE READY"
echo "---------------------------------"

echo "MongoDB:  $(pgrep -x mongod > /dev/null && echo RUNNING || echo FAILED)"
echo "Redis:    $(pgrep -x redis-server > /dev/null && echo RUNNING || echo FAILED)"
echo "Kafka:    $(pgrep -f kafka.Kafka > /dev/null && echo RUNNING || echo FAILED)"
echo "Zookeeper:$(pgrep -f QuorumPeerMain > /dev/null && echo RUNNING || echo FAILED)"

echo "---------------------------------"
echo "Kafka Topics:"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
echo "---------------------------------"

echo "📂 Logs stored in:"
echo "$LOG_DIR"