# Thinking in Streams: Building Stateful, Serverless Agentic Loops

[![Current 2026](https://img.shields.io/badge/Presented_at-Current_2026-blue)](https://current.confluent.io)
[![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-success)]()
[![Python](https://img.shields.io/badge/Python-3.10+-yellow)]()

This repository contains the live demonstration code for the session **"Thinking in Streams: Building Stateful, Serverless Agentic Loops"** presented at Current 2026 in London. 

It provides a deployable reference architecture that shifts AI agents from a synchronous, blocking "pull" model (standard REST/RAG) to an asynchronous, stateful "push" model using streaming infrastructure.

## 🎯 The Scenario: Autonomous Threat Interception

To prove that LLMs can operate safely and cheaply in the critical path of a high-throughput system, this demo simulates a global stream of authentication events. When a slow, distributed brute-force attack occurs, the system aggregates the state window, triggers a serverless AI agent, and neutralizes the threat using standardized Agent Skills—all while utilizing PyFlink to enforce idempotency and semantic caching.

### Key Innovations Demonstrated:
1. **Semantic Caching:** Bypassing costly LLM inference for mathematically similar attack signatures.
2. **Idempotent Execution:** Preventing agentic "Ghost Actions" (e.g., locking an account twice due to network retries).
3. **Stateful Memory:** Managing conversational/transactional context windows without static vector database lookups.

---

## 🏗️ Architecture Blueprint

* **The Nervous System:** Apache Kafka (Event ingestion and action ledger).
* **The Memory:** Apache Flink / PyFlink (Sliding windows, state management, and caching).
* **The Execution:** Google Cloud Run / FastAPI (Serverless, non-blocking LLM inference).
* **The Interface:** Open-standard Agent Skills (External tool execution).

---

## 📂 Repository Structure

```text
streaming-agentic-loop/
├── docker-compose.yml           # Local Kafka & Zookeeper environment
├── data-generator/
│   ├── requirements.txt
│   └── produce_stream.py        # Simulates global logins + botnet attack
├── flink-processor/
│   ├── requirements.txt
│   └── agent_loop.py            # PyFlink job: State, Cache, Idempotency
└── agent-cloudrun/
    ├── requirements.txt
    ├── Dockerfile
    └── main.py                  # FastAPI app: LLM Inference & Agent Skills
```

## 🚀 Quick Start & Live Demo Execution
To run this demo locally during a presentation, follow these steps. You will need Docker, Python 3.10+, and three separate terminal windows.

**Step 1: Start the Event Broker**
Spin up the local Kafka cluster.

```bash
docker-compose up -d
```
(Verify Kafka is running on localhost:9092 before proceeding).

**Step 2: Boot the Serverless Agent**
Start the FastAPI application that simulates the Cloud Run environment and executes the Agent Skills.

```bash
cd agent-cloudrun
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Step 3: Run the Stateful Engine**
Launch the PyFlink processor. This node manages the Semantic Cache and Idempotency states.

```bash
cd flink-processor
pip install -r requirements.txt
python agent_loop.py
```

**Step 4: Unleash the Chaos (The Demo Script)**
With the backend running, open a highly visible terminal window for the audience.

```bash
cd data-generator
pip install -r requirements.txt
python produce_stream.py
```


