import json, time, os
from typing import List, Dict, Any
from confluent_kafka import Consumer, Producer
from .config import (
    BROKERS, SOURCE_TOPIC, TARGET_TOPIC,
    MODEL_ID, AWS_REGION,
    BATCH_MAX_RECORDS, BATCH_MAX_MILLIS,
    INPUT_COLUMNS, OUTPUT_PREFIX
)
from .api import INSTRUCTION, OUTPUT_MAPPING
from .llm_batch import call_bedrock_batch
from .validator import validate_batch_response
from .merge import merge_outputs_into_record

def _build_input_item(i: int, rec: Dict[str,Any]) -> Dict[str,Any]:
    table = rec.get("source",{}).get("table","")
    after = rec.get("after", {}) or {}
    if INPUT_COLUMNS:
        cols = {k: after.get(k) for k in INPUT_COLUMNS if k in after}
    else:
        cols = after
    return {"i": i, "table": table, "columns": cols}

def run_loop():
    c = Consumer({"bootstrap.servers": BROKERS, "group.id":"llm-batch-transformer", "auto.offset.reset":"earliest"})
    p = Producer({"bootstrap.servers": BROKERS})
    c.subscribe([SOURCE_TOPIC])

    batch: List[Dict[str,Any]] = []
    raw_records: List[Dict[str,Any]] = []
    last_flush = time.time()

    processed = 0
    try:
        while True:
            msg = c.poll(0.05)
            now = time.time()

            if msg is not None:
                rec = json.loads(msg.value())
                raw_records.append(rec)
                batch.append(_build_input_item(len(batch), rec))
                processed += 1

            should_flush = False
            if len(batch) >= BATCH_MAX_RECORDS:
                should_flush = True
            elif (now - last_flush)*1000 >= BATCH_MAX_MILLIS and batch:
                should_flush = True

            if should_flush:
                instruction = INSTRUCTION or "Return outputs unchanged."  # safe default
                try:
                    # Only send fields the model needs: {i, columns:{...}}
                    slim_inputs = [{"i": item["i"], "columns": item["columns"]} for item in batch]
                    results = call_bedrock_batch(MODEL_ID, AWS_REGION, instruction, slim_inputs)
                    results = validate_batch_response(slim_inputs, results)
                except Exception as e:
                    # On model failure, pass-through unmodified
                    results = [{"i": i, "outputs": {}, "confidence": 0.0} for i in range(len(batch))]

                # merge and produce
                for i, rec in enumerate(raw_records):
                    outs = results[i]["outputs"] if i < len(results) else {}
                    merged = merge_outputs_into_record(rec, outs, OUTPUT_MAPPING, OUTPUT_PREFIX)
                    p.produce(TARGET_TOPIC, json.dumps(merged).encode("utf-8"))

                p.flush()
                batch.clear()
                raw_records.clear()
                last_flush = now

    finally:
        c.close()
