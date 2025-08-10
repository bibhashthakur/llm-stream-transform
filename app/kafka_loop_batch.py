import json, time
from typing import List, Dict, Any
from confluent_kafka import Consumer, Producer
from .config import (
    BROKERS, SOURCE_TOPIC, TARGET_TOPIC,  # TARGET_TOPIC can stay unused
    MODEL_ID, AWS_REGION,
    BATCH_MAX_RECORDS, BATCH_MAX_MILLIS,
    INPUT_COLUMNS, OUTPUT_PREFIX,
    WRITE_TO_KAFKA
)
from .s3_sink import S3Sink
from .api import INSTRUCTION, OUTPUT_MAPPING
from .llm_batch import call_bedrock_batch
from .validator import validate_batch_response
from .merge import merge_outputs_into_record

def run_loop():
    c = Consumer({"bootstrap.servers": BROKERS, "group.id": "llm-batch-transformer", "auto.offset.reset": "earliest"})
    p = Producer({"bootstrap.servers": BROKERS}) if WRITE_TO_KAFKA else None
    sink = S3Sink()

    c.subscribe([SOURCE_TOPIC])
    batch: List[Dict[str, Any]] = []
    raw_records: List[Dict[str, Any]] = []
    last_flush = time.time()

    try:
        while True:
            msg = c.poll(0.05)
            now = time.time()

            if msg is not None:
                rec = json.loads(msg.value())
                raw_records.append(rec)
                # keep index local to the batch
                i = len(batch)
                after = rec.get("after", {}) or {}
                cols = {k: after.get(k) for k in INPUT_COLUMNS} if INPUT_COLUMNS else after
                batch.append({"i": i, "columns": cols})

            should_flush = (
                len(batch) >= BATCH_MAX_RECORDS or
                ((now - last_flush) * 1000 >= BATCH_MAX_MILLIS and batch)
            )

            if should_flush:
                instruction = INSTRUCTION or "Return outputs unchanged."
                try:
                    results = call_bedrock_batch(MODEL_ID, AWS_REGION, instruction, batch)
                    results = validate_batch_response(batch, results)
                except Exception:
                    results = [{"i": i, "outputs": {}, "confidence": 0.0} for i in range(len(batch))]

                # Merge + S3 (and optionally Kafka)
                for i, rec in enumerate(raw_records):
                    outs = results[i]["outputs"] if i < len(results) else {}
                    merged = merge_outputs_into_record(rec, outs, OUTPUT_MAPPING, OUTPUT_PREFIX)

                    # S3 write (primary sink)
                    sink.write_record(merged)

                    # Optional Kafka produce
                    if WRITE_TO_KAFKA and p is not None:
                        p.produce(TARGET_TOPIC, json.dumps(merged).encode("utf-8"))

                if WRITE_TO_KAFKA and p is not None:
                    p.flush()

                if sink.should_flush():
                    sink.flush()

                batch.clear()
                raw_records.clear()
                last_flush = now
    finally:
        try:
            sink.close()
        finally:
            c.close()
