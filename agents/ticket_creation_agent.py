import datetime
import json
import random
import pymongo
from typing import TypedDict
import faiss
from sentence_transformers import SentenceTransformer
from pathlib import Path
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

############################################################
# FILE PATH
############################################################

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "data/ticket_creation.jsonl"
OUTPUT_FILE.parent.mkdir(exist_ok=True, parents=True)

############################################################
# MONGODB CONNECTION
############################################################

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["noc_alarm_system"]

devices_col = db["devices"]
services_col = db["services"]
engineers_col = db["engineers"]
shift_col = db["shift_schedule"]

############################################################
# SIMULATED LLM
############################################################

def llm(prompt):

    prompt_lower = prompt.lower()

    if "summary" in prompt_lower:
        text = prompt.split(":")[-1].strip()
        return f"Network monitoring detected the following incident: {text}. Immediate investigation is required."

    if "remediation" in prompt_lower or "resolution" in prompt_lower:

        cause = prompt.split(":")[-1].strip().lower()

        if "overheat" in cause or "fan" in cause:

            steps = [
                "1. Check router temperature sensors and confirm overheating condition",
                "2. Inspect cooling fans and verify they are functioning properly",
                "3. Clean dust from ventilation slots and improve airflow",
                "4. Replace faulty cooling fan if hardware issue is detected",
                "5. Continue monitoring device temperature to ensure stability"
            ]

        elif "link down" in cause or "fiber" in cause:

            steps = [
                "1. Verify physical fiber cable connectivity between devices",
                "2. Check optical signal levels on both ends of the interface",
                "3. Inspect fiber patch panel and connectors for damage",
                "4. Replace faulty fiber cable or SFP module if required",
                "5. Confirm link restoration and monitor interface stability"
            ]

        elif "power supply" in cause:
 
            steps = [
                "1. Check power supply unit status on the affected device",
                "2. Verify power cable connection and power source availability",
                "3. Switch to redundant PSU if available",
                "4. Replace faulty power supply hardware",
                "5. Monitor device uptime after power restoration"
            ]
 
        elif "bgp" in cause or "routing loop" in cause:
 
            steps = [
                "1. Review BGP routing table and identify abnormal routes",
                "2. Check recent routing configuration changes",
                "3. Clear BGP session or reset affected neighbors",
                "4. Apply route filtering or prefix limits if needed",
                "5. Monitor network traffic to confirm packet loss resolution"
            ]
 
        else:
            # Generic remediation fallback
            steps = [
                f"1. Investigate issue related to {cause}",
                "2. Verify device health and network interface status",
                "3. Restart affected network services or interfaces",
                "4. Analyze system logs for additional errors",
                "5. Escalate incident to network engineering team if unresolved"
            ]
 

        return "\n".join(steps)

    if "message" in prompt_lower:

        engineer = prompt.split("engineer")[-1].strip()

        return f"Alert: Network incident detected. Engineer {engineer} assigned."

    return "LLM response"


############################################################
# HELPERS (DB)
############################################################

def get_engineers():
    return list(engineers_col.find({}, {"_id": 0}))


def get_engineer_shift(name):

    shift = shift_col.find_one({"engineer": name})

    if shift:
        return shift["shift"]

    return None


############################################################
# SHIFT DETECTION
############################################################

def get_current_shift():

    hour = datetime.datetime.now().hour

    if 6 <= hour < 14:
        return "Morning"

    elif 14 <= hour < 22:
        return "Afternoon"

    return "Night"


############################################################
# ROUTING SCORE
############################################################

def routing_score(engineer, location, skill):

    score = 0

    if engineer["location"] == location:
        score += 5

    if skill in engineer["skills"]:
        score += 5

    score -= engineer["workload"]

    return score


############################################################
# KNOWLEDGE BASE
############################################################

kedb_docs = [
    "Router overheating due to fan failure",
    "Fiber cable cut causing link down",
    "Power supply failure in router",
    "BGP routing loop causing packet loss"
]

embed_model = SentenceTransformer("all-MiniLM-L6-v2")
vectors = embed_model.encode(kedb_docs)

dimension = vectors.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(vectors)


def search_kedb(text):

    q = embed_model.encode([text])
    D, I = index.search(q, 1)

    return kedb_docs[I[0][0]]


############################################################
# STATE
############################################################

class IncidentState(TypedDict):

    alarm: dict
    ticket: dict
    rag: str
    remediation: str
    dispatch: dict
    communication: dict


############################################################
# PRIORITY
############################################################

def classify_priority(severity, services):

    if severity == "CRITICAL" and len(services) >= 2:
        return "P1"

    if severity == "CRITICAL":
        return "P2"

    return "P3"


def classify_sla(priority):

    sla_map = {
        "P1": "1 Hour Response",
        "P2": "2 Hour Response",
        "P3": "4 Hour Response"
    }

    return sla_map.get(priority)


############################################################
# TICKET AGENT
############################################################

OPEN_TICKETS = []


def ticket_agent(state):

    alarm = state["alarm"]
    ci = alarm["ci"]

    device = devices_col.find_one({"device_id": ci})

    if not device:
        raise Exception(f"Device {ci} not found")

    services_cursor = services_col.find({"devices": ci})
    services = [svc["service_name"] for svc in services_cursor]

    location = device["location"]
    device_type = device.get("type", "router")

    summary = llm(f"Create summary: {alarm['text']}")

    root_cause = search_kedb(alarm["text"])

    priority = classify_priority(alarm["severity"], services)

    sla = classify_sla(priority)

    resolution = llm(f"Generate remediation for: {root_cause}")

    ticket = {

        "id": "INC-" + str(int(datetime.datetime.now().timestamp())),
        "ci": ci,
        "location": location,
        "services": services,
        "priority": priority,
        "sla": sla,
        "root_cause": root_cause,
        "summary": summary,
        "resolution": resolution,
        "logs": alarm["logs"],
        "created": str(datetime.datetime.now()),
        "status": "OPEN"
    }

    OPEN_TICKETS.append(ticket)

    print(f"\nTicket created for {ci}")

    return {"ticket": ticket, "alarm": alarm}


############################################################
# RAG
############################################################

def rag_agent(state):

    solution = search_kedb(state["alarm"]["text"])

    return {**state, "rag": solution}


############################################################
# REMEDIATION
############################################################

def remediation_agent(state):

    steps = llm(f"Generate remediation for: {state['rag']}")

    return {**state, "remediation": steps}


############################################################
# DISPATCH AGENT
############################################################

def dispatch_agent(state):

    ticket = state["ticket"]
    ci = ticket["ci"]
    location = ticket["location"]

    device = devices_col.find_one({"device_id": ci})

    required_skill = {

        "router": "Router",
        "switch": "Switch"

    }.get(device.get("type", "router"))

    engineers = get_engineers()

    current_shift = get_current_shift()

    available = [

        e for e in engineers
        if required_skill in e["skills"]
        and e["clock_in"]
        and get_engineer_shift(e["name"]) == current_shift
    ]

    if not available:

        available = [
            e for e in engineers
            if required_skill in e["skills"] and e["clock_in"]
        ]

    best = max(
        available,
        key=lambda e: routing_score(e, location, required_skill)
    )

    engineers_col.update_one(
        {"name": best["name"]},
        {"$inc": {"workload": 1}}
    )

    dispatch = {

        "engineer": best["name"],
        "group": "Network Team",
        "shift": get_engineer_shift(best["name"]),
        "routing_score": routing_score(best, location, required_skill),
        "field_force_required": ticket["priority"] == "P1"
    }

    return {**state, "dispatch": dispatch}


############################################################
# NOTIFICATION
############################################################
SMTP_SERVER = "smtp.zoho.com"
SMTP_PORT = 587

SENDER_EMAIL = "rasika.nale@acclivistechnologies.com"
SENDER_PASSWORD = "Pratik@!3Raa"
# def notify_engineer(ticket, dispatch):

#     engineer = engineers_col.find_one({"name": dispatch["engineer"]})

#     if not engineer:
#         return

#     print(f"\nSMS sent to {engineer['mobile']}")
#     print(f"Email sent to {engineer['email']}")

def notify_engineer(ticket, dispatch):

    engineer = engineers_col.find_one({"name": dispatch["engineer"]})

    if not engineer:
        print("Engineer not found")
        return

    engineer_email = engineer.get("email")
    engineer_mobile = engineer.get("mobile")

    print(f"\nSMS sent to {engineer_mobile}")

    # -------------------------
    # EMAIL MESSAGE
    # -------------------------

    subject = f"NOC ALERT: Incident {ticket['id']}"

    body = f"""
Hello {engineer['name']},

A new network incident has been assigned to you.

Incident ID: {ticket['id']}
Device: {ticket['ci']}
Location: {ticket['location']}
Priority: {ticket['priority']}
Services Impacted: {ticket['services']}

Root Cause:
{ticket['root_cause']}

Resolution Steps:
{ticket['resolution']}

Please investigate immediately.

Regards,
NOC Monitoring System
"""

    try:

        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = "Rasika.Nale@p3-group.com"
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()

        server.login(SENDER_EMAIL, SENDER_PASSWORD)

        server.sendmail(SENDER_EMAIL, "Rasika.Nale@p3-group.com", msg.as_string())

        server.quit()

        print(f"Email sent to {"Rasika.Nale@p3-group.com"}")

    except Exception as e:
        print("Email failed:", e)


############################################################
# LOG GENERATOR
############################################################

def generate_dynamic_logs(ci, text):

    base_time = datetime.datetime.now()

    logs = []

    def log(msg, seconds):

        t = (base_time + datetime.timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")

        logs.append(f"{t} | DEVICE:{ci} | {msg}")

    log("ALERT detected", 0)
    log("Monitoring system triggered alarm", 5)
    log("Network performance degraded", 10)

    return logs


############################################################
# COMMUNICATION
############################################################

def communication_agent(state):

    engineer = state["dispatch"]["engineer"]

    ticket = state["ticket"]

    message = llm(f"Generate message for engineer {engineer}")

    communication = {

        "message": message,
        "status_update": f"Incident {ticket['id']} created",
        "channels": ["Email", "Slack", "SMS"]
    }

    return {**state, "communication": communication}


############################################################
# DISPLAY PANEL
############################################################

def display_communication(state):

    print("\n==============================")
    print("INCIDENT COMMUNICATION PANEL")
    print("==============================")

    print("Ticket:", state["ticket"]["id"])
    print("Engineer:", state["dispatch"]["engineer"])
    print("Priority:", state["ticket"]["priority"])
    print("Shift:", state["dispatch"]["shift"])

    print("\nMessage:")
    print(state["communication"]["message"])


############################################################
# SAMPLE INCIDENTS
############################################################

sample_incidents = [

{
"ci": "core-1",
"text": "Router temperature critical. Interface down. Packet loss detected."
},

{
"ci": "acc-3",
"text": "Switch VLAN 10 down. Port not responding."
}

]


############################################################
# RUN PIPELINE
############################################################

for idx, incident in enumerate(sample_incidents):

    print("\nProcessing Incident", idx + 1)

    alarm = {

        "ci": incident["ci"],
        "alarm_id": f"ALARM-{500+idx}",
        "severity": "CRITICAL",
        "text": incident["text"],
        "logs": generate_dynamic_logs(incident["ci"], incident["text"]),
        "timestamp": str(datetime.datetime.now())
    }

    state = {"alarm": alarm}

    ticket_state = ticket_agent(state)

    rag_state = rag_agent(ticket_state)

    rem_state = remediation_agent(rag_state)

    dispatch_state = dispatch_agent(rem_state)

    notify_engineer(dispatch_state["ticket"], dispatch_state["dispatch"])

    comm_state = communication_agent(dispatch_state)

    ticket_output = {

        "ticket": comm_state["ticket"],
        "dispatch": comm_state["dispatch"],
        "communication": comm_state["communication"]
    }

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(ticket_output, default=str) + "\n")

    display_communication(comm_state)
