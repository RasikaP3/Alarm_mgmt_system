

PROJECT_DIR=~/agentic_noc_Final
cd "$PROJECT_DIR" || { echo "❌ Project directory not found"; exit 1; }

# -----------------------------
# STEP 0: Archive old files
# -----------------------------
# TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
# ARCHIVE_DIR="data/archive_$TIMESTAMP"
# echo "STEP 0 Archiving old data files to $ARCHIVE_DIR"

# mkdir -p "$ARCHIVE_DIR"

# List of files to archive
# FILES_TO_ARCHIVE=(
#     data/alarm_decisions.json
#     data/classifiedalarms_output.jsonl
#     data/communication_logs.json
#     data/monitoring_agent_output.jsonl
#     data/mtta_results.json
#     data/mttr_results.json
#     data/network_events.json
#     data/incident_topic_output.jsonl
# )

# for file in "${FILES_TO_ARCHIVE[@]}"; do
#     if [[ -f "$file" ]]; then
#         mv "$file" "$ARCHIVE_DIR/"
#         echo "📦 Moved $file → $ARCHIVE_DIR/"
#     fi
# done

# -----------------------------
# STEP 1: Activate venv
# -----------------------------
echo "STEP 1 Activating virtual environment"
source venv/bin/activate

python3 fault_generator.py
# -----------------------------
# STEP 2: Reset infrastructure
# -----------------------------
echo "STEP 2 Reset NOC stack"
./start_noc_stack.sh --reset
sleep 5

python3 reset_infra.py


# Terminal 1
python3 -m agents.Monitoring.monitoring_agent

# Terminal 2
python3 -m agents.classification.classification_agent

# Terminal 3
python3 -m agents.communication.communication_agent

# Terminal 4
python3 -m agents.orchestration.orchestration
