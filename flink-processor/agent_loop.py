from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time
import requests
import json

class AgenticLoopProcessor(KeyedProcessFunction):
    def __init__(self):
        self.semantic_cache = None
        self.action_ledger = None

    def open(self, runtime_context):
        # 1. Semantic Cache State (TTL: 1 hour)
        # Prevents sending similar threat patterns to the LLM multiple times
        cache_ttl = StateTtlConfig.new_builder(Time.hours(1)).set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite).build()
        cache_descriptor = ValueStateDescriptor("semantic_cache", Types.STRING())
        cache_descriptor.enable_time_to_live(cache_ttl)
        self.semantic_cache = runtime_context.get_state(cache_descriptor)

        # 2. Idempotency State (TTL: 24 hours)
        # Prevents "Ghost Actions" by tracking already quarantined IPs
        ledger_ttl = StateTtlConfig.new_builder(Time.hours(24)).set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite).build()
        ledger_descriptor = ValueStateDescriptor("action_ledger", Types.BOOLEAN())
        ledger_descriptor.enable_time_to_live(ledger_ttl)
        self.action_ledger = runtime_context.get_state(ledger_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        event = json.loads(value)
        ip_addr = event['ip_address']
        
        # IDEMPOTENCY CHECK: Have we already mitigated this IP?
        if self.action_ledger.value():
            return # Drop event, prevent ghost action
            
        # SEMANTIC CACHE CHECK: Have we seen this exact attack signature recently?
        # (In production, this would hash the state window properties)
        signature = f"brute_force_{event['username']}" 
        if self.semantic_cache.value() == signature:
            # Serve cached mitigation, bypass LLM
            yield json.dumps({"action": "quarantine_ip", "ip": ip_addr, "reason": "semantic_cache_hit", "token_cost": 0})
            return

        # If no cache hit, trigger the Serverless Agent (Cloud Run)
        # We pass the event (in reality, an aggregated window of events) to the agent
        try:
            # Calling our local FastAPI instance simulating Cloud Run
            response = requests.post(
                "http://localhost:8000/analyze", 
                json={"event_stream": [event]},
                timeout=2
            )
            decision = response.json()
            
            if decision.get("action") == "quarantine":
                # Update State to prevent ghost actions and cache future hits
                self.action_ledger.update(True)
                self.semantic_cache.update(signature)
                
                yield json.dumps({
                    "action": "quarantine_ip", 
                    "ip": ip_addr, 
                    "reason": "agent_decision", 
                    "llm_reasoning": decision.get("reasoning")
                })
        except requests.exceptions.RequestException as e:
            print(f"Agent unreachable: {e}")

# (Flink setup boilerplate omitted for brevity - connects Kafka consumer to processor and outputs to Kafka sink)
