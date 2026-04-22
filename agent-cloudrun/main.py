from fastapi import FastAPI
from pydantic import BaseModel
import json
from chdb.session import Session

app = FastAPI(title="Serverless Agentic Inference")

# ---------------------------------------------------------
# chDB Setup: Embedded, In-Memory Threat Intelligence DB
# ---------------------------------------------------------
db = Session()

@app.on_event("startup")
async def startup_event():
    """Initialize the in-memory ClickHouse table and seed threat data."""
    db.query("CREATE TABLE ip_reputation (ip String, risk_score UInt8) ENGINE = Memory")
    
    # Seeding the "known botnet" IPs used by our data-generator
    db.query("""
        INSERT INTO ip_reputation VALUES 
        ('192.168.45.12', 95), 
        ('192.168.45.13', 95), 
        ('192.168.45.14', 98)
    """)
    print("chDB initialized and seeded with threat intelligence.")

class EventPayload(BaseModel):
    event_stream: list[dict]

# ---------------------------------------------------------
# AGENT SKILL: IP Reputation Check 
# ---------------------------------------------------------
def skill_check_ip_reputation(ip_address: str) -> dict:
    """
    Standardized Agent Skill executing a live SQL query against 
    the embedded chDB engine to retrieve historical risk scores.
    """
    # Execute SQL via chDB, returning JSON
    query = f"SELECT risk_score FROM ip_reputation WHERE ip = '{ip_address}'"
    result = db.query(query, "JSON")
    
    data = result.data()
    
    # Parse result or default to low risk if IP is not in our threat DB
    if data and len(data) > 0:
        try:
            parsed = json.loads(data)
            if parsed.get("data"):
                score = parsed["data"][0].get("risk_score", 10)
                return {"ip": ip_address, "risk_score": score, "known_botnet": score > 90}
        except json.JSONDecodeError:
            pass
            
    return {"ip": ip_address, "risk_score": 10, "known_botnet": False}

@app.post("/analyze")
async def analyze_state_window(payload: EventPayload):
    events = payload.event_stream
    target_ip = events[0]["ip_address"]
    
    # 1. Agent executes Skill (Live chDB SQL Query)
    reputation_data = skill_check_ip_reputation(target_ip)
    
    # 2. Autonomous Reasoning Loop
    if reputation_data["known_botnet"]:
        return {
            "action": "quarantine",
            "reasoning": f"chDB lookup returned critical risk score: {reputation_data['risk_score']}. Coordinated attack confirmed.",
            "confidence": 0.99
        }
        
    return {"action": "monitor", "reasoning": "Normal traffic patterns observed."}
