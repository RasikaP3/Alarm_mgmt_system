"""
agents/communication_agent.py  — Communication Agent (Cross-Cutting)

Responsibilities (per spec):
  ✅ NOC dashboard print
  ✅ SMS / Email simulation (logged to result/notifications.jsonl)
  ✅ Enterprise customer message
  ✅ LLM-generated incident summary (NVIDIA → fallback)
  ✅ LLM-generated executive brief
  ✅ LLM-generated customer impact message
  ✅ Multi-language support (NOC_LANGUAGE env var)
  ✅ Conversational interface for NOC engineers (interactive mode)
  ✅ Results → result/communication_output.jsonl
               result/notifications.jsonl
  ✅ File-based fallback when Kafka unavailable
"""

import sys, os
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

import json, datetime, time, requests
from pathlib import Path
from config.settings import KAFKA_BOOTSTRAP, NVIDIA_API_KEY, NOC_LANGUAGE
from utils.result_writer import write_result, RESULT_DIR

IDLE_TIMEOUT = 120

# =========================================
# KAFKA
# =========================================
KAFKA_OK = False
consumer = None

try:
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "dispatch_topic",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="communication-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    KAFKA_OK = True
    print("✅ Kafka Connected")
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")
    print("ℹ️  Will fall back to reading result/dispatch_output.jsonl")

# =========================================
# LLM CALL
# =========================================
def llm_generate(prompt: str) -> str | None:
    if not NVIDIA_API_KEY:
        return None
    try:
        res = requests.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": "meta/llama3-70b-instruct",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 200, "temperature": 0.3},
            timeout=45,
        )
        if res.status_code != 200:
            return None
        data = res.json()
        if "choices" not in data:
            return None
        return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ LLM failed: {e}")
        return None

# =========================================
# MESSAGE GENERATORS
# =========================================

def safe_services(ticket: dict) -> str:
    val = ticket.get("impacted_services") or ticket.get("services_affected")
    if not val:
        return "N/A"
    if isinstance(val, list):
        return ", ".join(str(v) for v in val) if val else "N/A"
    return str(val)

def incident_summary(ticket: dict) -> str:
    p = (f"Write a concise technical summary in {NOC_LANGUAGE} "
         f"for issue '{ticket.get('root_cause')}' at {ticket.get('location')}.")
    return (llm_generate(p) or
            f"Issue: {ticket.get('root_cause')} detected at "
            f"{ticket.get('location')} affecting services.")
 
def executive_brief(ticket: dict) -> str:
    p = (f"Write a short executive summary in {NOC_LANGUAGE} "
         f"for a {ticket.get('priority')} incident at {ticket.get('location')}.")
    return (llm_generate(p) or
            f"{ticket.get('priority')} incident at {ticket.get('location')} "
            "is under active investigation.")
 
def customer_message(ticket: dict) -> str:
    # use existing message from alarm logs first
    for log in ticket.get("alarm_logs", []):
        if isinstance(log, dict) and log.get("customer_message"):
            return log["customer_message"]
    p = f"Write a polite customer-facing service disruption message in {NOC_LANGUAGE}."
    return (llm_generate(p) or
            "We are working to resolve a service disruption. "
            "Thank you for your patience.")
 


# =========================================
# NOTIFICATIONS SIMULATION
# (spec: SMS / Email / Enterprise customer)
# =========================================
def send_notifications(ticket: dict, summary: str, cust_msg: str):
    """Simulates SMS, email and enterprise customer notification."""
    now = datetime.datetime.now().isoformat()
    notifications = []

    # NOC engineer SMS
    notifications.append({
        "channel":   "SMS",
        "recipient": "NOC_On_Call",
        "message":   f"[{ticket.get('priority')}] {ticket.get('device_name')} "
                     f"@ {ticket.get('location')} — {ticket.get('root_cause')}",
        "timestamp": now,
    })

    # Ops manager email
    notifications.append({
        "channel":   "Email",
        "recipient": "ops-manager@company.com",
        "subject":   f"[{ticket.get('priority')}] Incident {ticket.get('ticket_id')}",
        "body":      summary,
        "timestamp": now,
    })

    # Enterprise customer
    if ticket.get("customers_affected"):
        notifications.append({
            "channel":   "CustomerPortal",
            "recipient": "enterprise_customers",
            "message":   cust_msg,
            "timestamp": now,
        })

    for n in notifications:
        write_result("notifications.jsonl", n)
        print(f"  📨 Notification → {n['channel']} : {n['recipient']}")

    return notifications

# =========================================
# DASHBOARD
# =========================================
def show_dashboard(ticket: dict, engineer: dict,
                   summary: str, brief: str, cust_msg: str):
    print("\n" + "=" * 55)
    print("  NOC INCIDENT DASHBOARD")
    print("=" * 55)
    print(f"  Ticket ID  : {ticket.get('ticket_id')}")
    print(f"  Incident   : {ticket.get('incident_id')}")
    print(f"  Device     : {ticket.get('device_name')} ({ticket.get('location')})")
    print(f"  Priority   : {ticket.get('priority')}  |  Severity : {ticket.get('severity')}")
    print(f"  Root Cause : {ticket.get('root_cause')}")
    print(f"  Services   : {safe_services(ticket)}")
    print(f"  Customers  : {ticket.get('customers_affected', 'N/A')}")
    print(f"  SLA        : {ticket.get('sla_due_time')}")
    print(f"  Engineer   : {engineer.get('name')}  [{engineer.get('skill')} / {engineer.get('shift')}]")

    print("\n  --- Alarm Logs ---")
    logs = ticket.get("alarm_logs", [])
    if logs:
        for log in logs:
            if isinstance(log, dict):
                ts  = log.get("timestamp", "N/A")
                sev = (log.get("severity") or "INFO").upper()
                msg = log.get("message") or log.get("alarm_type") or "No message"
                print(f"    [{ts}]  {sev}  —  {msg}")
    else:
        print("    No alarm logs attached.")

    print("\n  --- Technical Summary ---")
    print(f"  {summary}")

    print("\n  --- Executive Brief ---")
    print(f"  {brief}")

    print("\n  --- Customer Message ---")
    print(f"  {cust_msg}")

    print("\n  --- Resolution Steps ---")
    for step in (ticket.get("resolution_steps") or ["No steps available"]):
        print(f"    • {step}")
    print("=" * 55)

# =========================================
# CONVERSATIONAL NOC INTERFACE
# (spec: conversational interface for NOC engineers)
# =========================================
def noc_chat_session(last_ticket: dict):
    """Simple interactive Q&A for NOC engineers about the last processed incident."""
    print("\n💬 NOC Conversational Interface (type 'exit' to quit)")
    context = json.dumps(last_ticket, default=str)
    history = []

    while True:
        try:
            query = input("NOC> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if query.lower() in ("exit", "quit", "q"):
            break
        if not query:
            continue

        history.append({"role": "user", "content": query})
        prompt = (
            f"You are a NOC assistant. Use this incident context:\n{context}\n\n"
            + "\n".join(f"{m['role'].upper()}: {m['content']}" for m in history[-6:])
        )
        response = llm_generate(prompt) or "I don't have enough information to answer that."
        history.append({"role": "assistant", "content": response})
        print(f"  Assistant: {response}\n")


# =========================================
# PROCESS ONE DISPATCH MESSAGE
# =========================================
def handle(data: dict) -> dict:
    ticket   = data.get("ticket", {})
    engineer = data.get("engineer", {})

    summary  = incident_summary(ticket)
    brief    = executive_brief(ticket)
    cust_msg = customer_message(ticket)

    show_dashboard(ticket, engineer, summary, brief, cust_msg)
    send_notifications(ticket, summary, cust_msg)

    output = {
        "ticket_id":        ticket.get("ticket_id"),
        "incident_id":      ticket.get("incident_id"),
        "device_name":      ticket.get("device_name"),
        "location":         ticket.get("location"),
        "priority":         ticket.get("priority"),
        "summary":          summary,
        "executive_brief":  brief,
        "customer_message": cust_msg,
        "engineer":         engineer,
        "language":         NOC_LANGUAGE,
        "timestamp":        datetime.datetime.now().isoformat(),
    }

    write_result("communication_output.jsonl", output)
    return output


# =========================================
# MAIN
# =========================================
def run():
    print("📣 Communication Agent started...")
    last_output = None

    # ── MODE 1: Kafka ──────────────────────────────────────
    if KAFKA_OK and consumer:
        last = time.time()
        try:
            while True:
                records = consumer.poll(timeout_ms=1000)
                if records:
                    for _, messages in records.items():
                        for msg in messages:
                            last_output = handle(msg.value)
                            last = time.time()
                else:
                    if time.time() - last > IDLE_TIMEOUT:
                        print("\n🛑 No messages for 15 s. Stopping.")
                        break
        finally:
            consumer.close()

    # ── MODE 2: File fallback ──────────────────────────────
    else:
        src = RESULT_DIR / "dispatch_output.jsonl"
        if not src.exists():
            print(f"⚠️  No Kafka and no {src}. Run dispatch_agent first.")
            return
        print(f"📂 Reading from {src}")
        with open(src, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    last_output = handle(json.loads(line))

    print("✅ Communication Agent stopped.")
    print("📁 Results → result/communication_output.jsonl")
    print("📁 Notifications → result/notifications.jsonl")

    # Launch NOC chat if interactive terminal
    if last_output and sys.stdin.isatty():
        noc_chat_session(last_output)


if __name__ == "__main__":
    run()