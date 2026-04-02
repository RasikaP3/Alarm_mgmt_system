"""
agents/communication_agent.py  — Communication Agent (Cross-Cutting)

Responsibilities (per spec):
  ✅ NOC dashboard print (formatted, full alarm log display)
  ✅ SMS / Email simulation (logged to result/notifications.jsonl)
  ✅ Enterprise customer notification (webhook + log)
  ✅ LLM-generated incident summary
  ✅ LLM-generated executive brief
  ✅ LLM-generated customer impact message
  ✅ Context-aware tone per priority level
  ✅ Multi-language support (NOC_LANGUAGE env var)
  ✅ Conversational interface for NOC engineers (interactive terminal)
  ✅ Results → result/communication_output.jsonl
               result/notifications.jsonl
  ✅ File-based fallback when Kafka unavailable

Fixes applied vs original:
  FIX-1  noc_chat_session receives the full dispatch record (ticket + engineer + summaries),
           not just last_output — engineers now chat about the actual incident
  FIX-2  customers_affected guard handles int 0 / string "0" / empty list
  FIX-3  IDLE_TIMEOUT log message uses the constant — was always printing "15 s"
  FIX-4  Context-aware tone: P1 → urgent/direct, P2 → assertive, P3/P4 → informational
  FIX-5  Customer webhook delivery with retry (3 attempts, 2s back-off)
  FIX-6  Chat history passed correctly to LLM — no stale context accumulation
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

IDLE_TIMEOUT         = 150
CUSTOMER_WEBHOOK_URL = os.getenv("CUSTOMER_WEBHOOK_URL", "")

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
# TONE MAP  (FIX-4: context-aware tone per priority)
# =========================================
TONE_DIRECTIVE = {
    "P1": (
        "Use urgent, direct, action-oriented language. No fluff. "
        "Every sentence must convey severity and drive immediate action."
    ),
    "P2": (
        "Use assertive, professional language. Communicate urgency clearly "
        "but avoid alarm. Focus on impact and resolution timeline."
    ),
    "P3": (
        "Use calm, informative language. Acknowledge the issue and "
        "set realistic expectations for resolution."
    ),
    "P4": (
        "Use factual, routine language. Keep it brief. "
        "Low urgency — informational only."
    ),
}


# =========================================
# LLM CALL
# =========================================
def llm_generate(prompt: str, max_tokens: int = 200) -> str | None:
    if not NVIDIA_API_KEY:
        return None
    try:
        res = requests.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}",
                     "Content-Type": "application/json"},
            json={
                "model":       "meta/llama3-70b-instruct",
                "messages":    [{"role": "user", "content": prompt}],
                "max_tokens":  max_tokens,
                "temperature": 0.3,
            },
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


def llm_chat(messages: list, max_tokens: int = 300) -> str | None:
    """Multi-turn LLM call with full history."""
    if not NVIDIA_API_KEY:
        return None
    try:
        res = requests.post(
            "https://integrate.api.nvidia.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {NVIDIA_API_KEY}",
                     "Content-Type": "application/json"},
            json={
                "model":       "meta/llama3-70b-instruct",
                "messages":    messages,
                "max_tokens":  max_tokens,
                "temperature": 0.4,
            },
            timeout=45,
        )
        if res.status_code != 200:
            return None
        return res.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"⚠️ Chat LLM failed: {e}")
        return None


# =========================================
# HELPERS
# =========================================
def safe_services(ticket: dict) -> str:
    val = ticket.get("impacted_services") or ticket.get("services_affected")
    if not val:
        return "N/A"
    if isinstance(val, list):
        return ", ".join(str(v) for v in val) if val else "N/A"
    return str(val)

def customers_affected_truthy(ticket: dict) -> bool:          # FIX-2
    val = ticket.get("customers_affected")
    return val not in (None, 0, "0", [], "", "None")

def tone_for(priority: str) -> str:                           # FIX-4
    return TONE_DIRECTIVE.get(priority, TONE_DIRECTIVE["P4"])


# =========================================
# MESSAGE GENERATORS
# =========================================
def incident_summary(ticket: dict) -> str:
    tone = tone_for(ticket.get("priority", "P4"))
    p = (
        f"{tone} "
        f"Write a concise technical summary in {NOC_LANGUAGE} for a NOC incident. "
        f"Device: {ticket.get('device_name')}. "
        f"Location: {ticket.get('location')}. "
        f"Root cause: {ticket.get('root_cause')}. "
        f"Priority: {ticket.get('priority')}. "
        f"Services affected: {safe_services(ticket)}."
    )
    return (llm_generate(p) or
            f"[{ticket.get('priority')}] Issue on {ticket.get('device_name')} "
            f"at {ticket.get('location')}: {ticket.get('root_cause')}. "
            f"Services affected: {safe_services(ticket)}.")


def executive_brief(ticket: dict) -> str:
    tone = tone_for(ticket.get("priority", "P4"))
    p = (
        f"{tone} "
        f"Write a short executive summary in {NOC_LANGUAGE} for senior management. "
        f"Incident priority: {ticket.get('priority')}. "
        f"Location: {ticket.get('location')}. "
        f"Customer impact: {'Yes' if customers_affected_truthy(ticket) else 'No'}. "
        f"Root cause: {ticket.get('root_cause')}. "
        f"SLA due: {ticket.get('sla_due_time')}."
    )
    return (llm_generate(p) or
            f"{ticket.get('priority')} incident at {ticket.get('location')} "
            "is under active investigation by the NOC team.")


def customer_message(ticket: dict) -> str:
    # Use embedded message from alarm logs if available
    for log in ticket.get("alarm_logs", []):
        if isinstance(log, dict) and log.get("customer_message"):
            return log["customer_message"]

    tone = tone_for(ticket.get("priority", "P4"))
    p = (
        f"{tone} "
        f"Write a polite, professional customer-facing service disruption notice "
        f"in {NOC_LANGUAGE}. Do not reveal internal system names or technical details. "
        f"Services impacted: {safe_services(ticket)}. "
        f"Expected resolution target based on {ticket.get('priority')} priority."
    )
    return (llm_generate(p) or
            "We are currently investigating a service disruption affecting some of our services. "
            "Our team is working to restore full service as quickly as possible. "
            "We apologise for any inconvenience caused.")


# =========================================
# CUSTOMER WEBHOOK DELIVERY  (FIX-5)
# =========================================
def deliver_customer_webhook(message: str, ticket: dict, retries: int = 3):
    if not CUSTOMER_WEBHOOK_URL:
        return
    payload = {
        "incident_id":     ticket.get("incident_id"),
        "ticket_id":       ticket.get("ticket_id"),
        "message":         message,
        "priority":        ticket.get("priority"),
        "timestamp":       datetime.datetime.now().isoformat(),
    }
    for attempt in range(1, retries + 1):
        try:
            res = requests.post(CUSTOMER_WEBHOOK_URL, json=payload, timeout=5)
            print(f"  🌐 Customer webhook → HTTP {res.status_code}")
            return
        except Exception as e:
            print(f"  ⚠️  Customer webhook attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** (attempt - 1))   # 1s, 2s back-off


# =========================================
# NOTIFICATIONS
# =========================================
def send_notifications(ticket: dict, summary: str, cust_msg: str) -> list:
    """Simulate SMS, email, and enterprise customer notification."""
    now = datetime.datetime.now().isoformat()
    notifications = []

    # NOC engineer SMS
    notifications.append({
        "channel":   "SMS",
        "recipient": "NOC_On_Call",
        "message":   (
            f"[{ticket.get('priority')}] {ticket.get('device_name')} "
            f"@ {ticket.get('location')} — {ticket.get('root_cause')}"
        ),
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

    # Enterprise customer (FIX-2: proper guard)
    if customers_affected_truthy(ticket):
        notifications.append({
            "channel":   "CustomerPortal",
            "recipient": "enterprise_customers",
            "message":   cust_msg,
            "timestamp": now,
        })
        deliver_customer_webhook(cust_msg, ticket)          # FIX-5

    for n in notifications:
        write_result("notifications.jsonl", n)
        print(f"  📨 {n['channel']} → {n['recipient']}")

    return notifications


# =========================================
# DASHBOARD
# =========================================
def show_dashboard(ticket: dict, engineer: dict,
                   summary: str, brief: str, cust_msg: str):

    sep = "=" * 70
    sub = "-" * 70

    def format_block(text):
        """Split long paragraphs into readable lines"""
        if not text:
            return "  N/A"
        lines = str(text).split("\n")
        return "\n".join(f"  {line.strip()}" for line in lines if line.strip())

    print(f"\n{sep}")
    print("                 🚨 NOC INCIDENT DASHBOARD 🚨")
    print(sep)

    print(f"\n🔹 Ticket Info")
    print(sub)
    print(f"  Ticket ID     : {ticket.get('ticket_id')}")
    print(f"  Incident ID   : {ticket.get('incident_id')}")
    print(f"  Device        : {ticket.get('device_name')}  ({ticket.get('location')})")

    print(f"\n🔹 Priority & Impact")
    print(sub)
    print(f"  Priority      : {ticket.get('priority')}")
    print(f"  Severity      : {ticket.get('severity')}")
    print(f"  Category      : {ticket.get('category', 'N/A')}")
    print(f"  Root Cause    : {ticket.get('root_cause')}")

    print(f"\n🔹 Impact Details")
    print(sub)
    print(f"  Services      : {safe_services(ticket)}")
    print(f"  Customers     : {ticket.get('customers_affected', 'N/A')}")
    print(f"  SLA           : {ticket.get('sla_due_time')}  [{ticket.get('sla_level', 'N/A')}]")

    print(f"\n🔹 Assigned Engineer")
    print(sub)
    print(f"  Name          : {engineer.get('name')}")
    print(f"  Skill         : {engineer.get('skill')}")
    print(f"  Shift         : {engineer.get('shift')}")

    print(f"\n🔹 Alarm Logs")
    print(sub)
    logs = ticket.get("alarm_logs", [])
    if logs:
        for log in logs:
            if isinstance(log, dict):
                ts  = log.get("timestamp", "N/A")
                sev = (log.get("severity") or "INFO").upper()
                msg = log.get("message") or log.get("alarm_type") or "No message"
                print(f"  [{ts}]  {sev:8s}  →  {msg}")
    else:
        print("  No alarm logs available")

    print(f"\n🔹 Technical Summary")
    print(sub)
    print(format_block(summary))

    print(f"\n🔹 Executive Brief")
    print(sub)
    print(format_block(brief))

    print(f"\n🔹 Customer Communication")
    print(sub)
    print(format_block(cust_msg))

    print(f"\n🔹 Resolution Steps")
    print(sub)
    steps = ticket.get("resolution_steps") or []
    if steps:
        for i, step in enumerate(steps, 1):
            print(f"  {i}. {step}")
    else:
        print("  No steps available")

    print(f"\n🔹 Recommended Action")
    print(sub)
    print(format_block(ticket.get('recommended_action', 'N/A')))

    print(f"\n{sep}\n")


# =========================================
# CONVERSATIONAL NOC INTERFACE  (FIX-1, FIX-6)
# =========================================
def noc_chat_session(dispatch_record: dict):
    """
    Interactive Q&A for NOC engineers about the processed incident.
    Uses the full dispatch record (ticket + engineer + summaries) as context.
    History is maintained and passed as proper message array to LLM.   (FIX-6)
    """
    print("\n💬 NOC Conversational Interface  (type 'exit' to quit)")
    print("   Ask anything about this incident — root cause, resolution steps,")
    print("   engineer details, SLA status, customer impact, etc.\n")

    # Build a rich system context from the full dispatch record  (FIX-1)
    ticket   = dispatch_record.get("ticket", dispatch_record)
    engineer = dispatch_record.get("engineer", {})

    system_ctx = (
        "You are an expert NOC assistant. Answer questions about the following incident. "
        "Be concise, technically precise, and action-oriented.\n\n"
        f"INCIDENT CONTEXT:\n{json.dumps(dispatch_record, default=str, indent=2)}"
    )

    # Full message history for multi-turn LLM calls   (FIX-6)
    history = [{"role": "system", "content": system_ctx}]

    print(f"  Context loaded: {ticket.get('ticket_id')} | "
          f"{ticket.get('device_name')} | {ticket.get('priority')}")
    print(f"  Assigned engineer: {engineer.get('name')} ({engineer.get('skill')})\n")

    while True:
        try:
            query = input("NOC> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if query.lower() in ("exit", "quit", "q", ""):
            if not query:
                continue
            break

        history.append({"role": "user", "content": query})

        response = (
            llm_chat(history[-12:])     # keep last 12 turns to stay within token budget
            or "I don't have enough information to answer that. "
               "Check the incident context above."
        )

        history.append({"role": "assistant", "content": response})
        print(f"\n  Assistant: {response}\n")


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
        "severity":         ticket.get("severity"),
        "summary":          summary,
        "executive_brief":  brief,
        "customer_message": cust_msg,
        "engineer":         engineer,
        "language":         NOC_LANGUAGE,
        "timestamp":        datetime.datetime.now().isoformat(),
        # Preserve full record for chat context  (FIX-1)
        "_dispatch_record": data,
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
                        # FIX-3: use constant in message
                        print(f"\n🛑 No messages for {IDLE_TIMEOUT}s. Stopping.")
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

    # Launch NOC chat if running in an interactive terminal  (FIX-1)
    if last_output and sys.stdin.isatty():
        noc_chat_session(last_output.get("_dispatch_record", last_output))


if __name__ == "__main__":
    run()