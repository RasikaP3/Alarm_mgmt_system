@echo off
:: ============================================================
:: NOC MULTI-AGENT SYSTEM — STOP ALL
:: ============================================================

echo Stopping all NOC agent windows...
taskkill /FI "WINDOWTITLE eq INCIDENT AGENT*"     /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq TICKET AGENT*"       /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq DISPATCH AGENT*"     /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq COMMUNICATION AGENT*"/F >nul 2>&1
taskkill /FI "WINDOWTITLE eq ORCHESTRATION AGENT*"/F >nul 2>&1

echo Stopping Kafka Broker...
taskkill /FI "WINDOWTITLE eq KAFKA BROKER*" /F >nul 2>&1
timeout /t 3 /nobreak >nul

echo Stopping Zookeeper...
taskkill /FI "WINDOWTITLE eq ZOOKEEPER*" /F >nul 2>&1

echo.
echo All NOC services stopped.
pause
