from agents.Monitoring.monitoring_agent import  run_monitoring_from_file
from agents.classification.classification_agent import run_classification
from agents.communication.communication_agent import run_communication
from agents.orchestration.orchestration import run_orchestrator

if __name__ == "__main__":
     run_monitoring_from_file()
     run_classification()
     run_communication()
     run_orchestrator()