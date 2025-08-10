# Dockerfile
FROM python:3.11-slim

# System deps (librdkafka for confluent-kafka; curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 curl gcc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app /app/app
# Install Python deps
RUN pip install --no-cache-dir fastapi uvicorn confluent-kafka pydantic boto3

ENV PORT=8080
CMD ["python","-m","app.main"]
