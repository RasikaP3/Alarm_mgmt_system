#!/bin/bash

echo "-----------------------------------------"
echo "🚀 NOC TEST PIPELINE STARTED"
echo "-----------------------------------------"

PROJECT_DIR=~/agentic_noc_Final
cd "$PROJECT_DIR" || { echo "❌ Project directory not found"; exit 1; }

# -----------------------------
# STEP 0: Clean old files
# -----------------------------
echo "STEP 0️⃣ Deleting old data files"
rm -f data/alarm_decisions.json \
      data/classified_alarms_output.jsonl \
      data/communication_logs.json \
      data/monitoring_agent_output.jsonl \
      data/mtta_results.json \
      data/mttr_results.json \
      data/network_events.json \
      data/incident_topic_output.jsonl

# -----------------------------
# STEP 1: Activate venv
# -----------------------------
echo "STEP 1️⃣ Activating virtual environment"
source venv/bin/activate

# -----------------------------
# STEP 2: Reset infrastructure
# -----------------------------
echo "STEP 2️⃣ Reset NOC stack"
./start_noc_stack.sh --reset
sleep 5

python3 reset_infra.py

# -----------------------------
# STEP 3: Generate Faults
# -----------------------------
echo "STEP 3️⃣ Generating Fault Events"
python3 fault_generator.py

# -----------------------------
# STEP 4: Setup Database
# -----------------------------
echo "STEP 4️⃣ Setup NOC Database"
./setup_noc_db.sh
sleep 2

# -----------------------------
# STEP 5: Run agents sequentially (produce input files first)
# -----------------------------
echo "STEP 5️⃣ Run Monitoring Agent"
python3 agents/monitoring_agent.py

echo "STEP 6️⃣ Run Classification Agent"
python3 agents/classification_agent.py

echo "STEP 7️⃣ Run Flapping/Topology/Super Agent"
python3 agents/flap_topology_super_agent.py

# -----------------------------
# STEP 6: Run real-time agents in background
# -----------------------------
echo "STEP 8️⃣ Start Communication Agent (Kafka listener)"
python3 agents/communication_agent.py &

echo "STEP 9️⃣ Start Incident Management Agent (Kafka listener)"
python3 agents/incident_mgmt_agent.py &

echo "STEP 10 Start ticket creation"
python3 agents/ticket_creation_agent.py
echo ""
echo "-----------------------------------------"
echo "✅ NOC TEST PIPELINE COMPLETED"
echo "-----------------------------------------"