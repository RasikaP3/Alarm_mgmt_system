@echo off
:: ============================================================
:: NOC MULTI-AGENT SYSTEM — ONE SHOT LAUNCHER
:: Place this file at D:\alarmclaude\start_noc.bat
:: ============================================================

set KAFKA_DIR=D:\kafka_2.13-3.5.1
set PROJECT_DIR=D:\alarmclaude
set VENV=D:\alarmclaude\agents\noc_env_new\Scripts\activate.bat

echo.
echo ============================================================
echo   NOC MULTI-AGENT SYSTEM LAUNCHER
echo ============================================================
echo.

:: ── STEP 1: Zookeeper ───────────────────────────────────────
echo [1/8] Starting Zookeeper...
start "ZOOKEEPER" cmd /k "cd /d %KAFKA_DIR% && bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
timeout /t 5 /nobreak >nul

:: ── STEP 2: Kafka Broker ────────────────────────────────────
echo [2/8] Starting Kafka Broker...
start "KAFKA BROKER" cmd /k "cd /d %KAFKA_DIR% && bin\windows\kafka-server-start.bat config\server.properties"
timeout /t 8 /nobreak >nul

:: ── STEP 3: Create Topics ───────────────────────────────────
echo [3/8] Creating Kafka Topics...
start "KAFKA TOPICS" cmd /k "cd /d %KAFKA_DIR% && bin\windows\kafka-topics.bat --create --if-not-exists --topic incident_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && bin\windows\kafka-topics.bat --create --if-not-exists --topic incident_to_ticket --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && bin\windows\kafka-topics.bat --create --if-not-exists --topic ticket_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && bin\windows\kafka-topics.bat --create --if-not-exists --topic dispatch_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && bin\windows\kafka-topics.bat --create --if-not-exists --topic orchestrated_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && bin\windows\kafka-topics.bat --create --if-not-exists --topic field_force_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 && echo. && echo All topics created! && timeout /t 3 /nobreak >nul"
timeout /t 12 /nobreak >nul

:: ── STEP 4: Incident Agent ──────────────────────────────────
echo [4/8] Starting Incident Agent...
start "INCIDENT AGENT" cmd /k "cd /d %PROJECT_DIR% && call %VENV% && python agents\incident_agent.py && echo. && echo Incident Agent finished. && pause"
timeout /t 6 /nobreak >nul

:: ── STEP 5: Ticket Agent ────────────────────────────────────
echo [5/8] Starting Ticket Agent...
start "TICKET AGENT" cmd /k "cd /d %PROJECT_DIR% && call %VENV% && python agents\ticket_agent.py && echo. && echo Ticket Agent finished. && pause"
timeout /t 4 /nobreak >nul

:: ── STEP 6: Dispatch Agent ──────────────────────────────────
echo [6/8] Starting Dispatch Agent...
start "DISPATCH AGENT" cmd /k "cd /d %PROJECT_DIR% && call %VENV% && python agents\dispatch_agent.py && echo. && echo Dispatch Agent finished. && pause"
timeout /t 4 /nobreak >nul

:: ── STEP 7: Communication Agent ─────────────────────────────
echo [7/8] Starting Communication Agent...
start "COMMUNICATION AGENT" cmd /k "cd /d %PROJECT_DIR% && call %VENV% && python agents\communication_agent.py && echo. && echo Communication Agent finished. && pause"
timeout /t 4 /nobreak >nul

:: ── STEP 8: Orchestration Agent ─────────────────────────────
echo [8/8] Starting Orchestration Agent...
start "ORCHESTRATION AGENT" cmd /k "cd /d %PROJECT_DIR% && call %VENV% && python agents\orchestration_agent.py && echo. && echo Orchestration Agent finished. && pause"

echo.
echo ============================================================
echo   ALL SERVICES LAUNCHED
echo ============================================================
echo.
echo   Window 1  : Zookeeper          (keep open always)
echo   Window 2  : Kafka Broker       (keep open always)
echo   Window 3  : Kafka Topics       (closes after creation)
echo   Window 4  : Incident Agent
echo   Window 5  : Ticket Agent
echo   Window 6  : Dispatch Agent
echo   Window 7  : Communication Agent
echo   Window 8  : Orchestration Agent
echo.
echo   Results saved to: %PROJECT_DIR%\result\
echo.
echo   To stop everything: run stop_noc.bat
echo.
pause
