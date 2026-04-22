import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'auth_events'

# The "Threat" IPs - a distributed botnet
MALICIOUS_IPS = ["192.168.45.12", "192.168.45.13", "192.168.45.14"]
TARGET_ACCOUNT = "admin_svc_acct"

def generate_event(is_malicious=False):
    if is_malicious:
        ip = random.choice(MALICIOUS_IPS)
        user = TARGET_ACCOUNT
        status = "FAILED"
        user_agent = "Mozilla/5.0 (Unknown; Linux x86_64) headless"
    else:
        ip = f"{random.randint(10, 200)}.{random.randint(0, 255)}.0.{random.randint(1, 254)}"
        user = f"user_{random.randint(1000, 9999)}"
        status = random.choices(["SUCCESS", "FAILED"], weights=[0.9, 0.1])[0]
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

    return {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ip_address": ip,
        "username": user,
        "status": status,
        "user_agent": user_agent,
        "datacenter": random.choice(["eu-west-1", "us-east-1", "ap-south-1"])
    }

print(f"Streaming data to {TOPIC}...")
try:
    while True:
        # Inject an anomaly 10% of the time to trigger the Flink window
        is_attack = random.random() < 0.10
        event = generate_event(is_attack)
        
        producer.send(TOPIC, key=event["username"].encode('utf-8'), value=event)
        
        # Print to console for visual effect during demo
        color = "\033[91m" if is_attack else "\033[92m" # Red for attack, Green for normal
        print(f"{color}Produced: {event['username']} from {event['ip_address']} [{event['status']}]\033[0m")
        
        time.sleep(0.2) # 5 events per second
except KeyboardInterrupt:
    producer.close()
