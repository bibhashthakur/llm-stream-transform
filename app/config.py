import os
BROKERS = os.getenv("KAFKA_BROKERS","localhost:9092")
SOURCE_TOPIC = os.getenv("SOURCE_TOPIC","rds.changes")
TARGET_TOPIC = os.getenv("TARGET_TOPIC","transformed.changes")
MODEL_ID = os.getenv("BEDROCK_MODEL","anthropic.claude-3-5-sonnet-20240620")
AWS_REGION = os.getenv("AWS_REGION","us-east-1")