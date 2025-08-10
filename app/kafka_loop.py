import json, threading
from confluent_kafka import Consumer, Producer
from .config import BROKERS, SOURCE_TOPIC, TARGET_TOPIC
from .api import get_current_plan
from .transformer import transform_record

def run_loop():
    c = Consumer({"bootstrap.servers": BROKERS, "group.id":"llm-transformer","auto.offset.reset":"earliest"})
    p = Producer({"bootstrap.servers": BROKERS})
    c.subscribe([SOURCE_TOPIC])
    processed = transformed = errors = 0

    try:
        while True:
            msg = c.poll(0.5)
            if not msg: continue
            try:
                processed += 1
                rec = json.loads(msg.value())
                plan = get_current_plan()
                if plan:
                    out = transform_record(rec, plan)
                else:
                    out = rec
                p.produce(TARGET_TOPIC, json.dumps(out).encode("utf-8"))
                transformed += 1
            except Exception:
                errors += 1
    finally:
        c.close()
