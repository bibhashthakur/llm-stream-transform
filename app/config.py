import os

BROKERS = os.getenv("KAFKA_BROKERS", "127.0.0.1:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC", "rds.changes")
TARGET_TOPIC = os.getenv("TARGET_TOPIC", "transformed.changes")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
MODEL_ID = os.getenv("BEDROCK_MODEL", "anthropic.claude-3-5-sonnet-20240620")

# batching
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "100"))
BATCH_MAX_MILLIS = int(os.getenv("BATCH_MAX_MILLIS", "250"))

# which columns to send to the model (comma-separated). Empty = send all of "after".
INPUT_COLUMNS = [c.strip() for c in os.getenv("BATCH_INPUT_COLUMNS", "").split(",") if c.strip()]

# where to write model outputs. "prefix" or "mapping"
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "llm_")  # used if no mapping provided
