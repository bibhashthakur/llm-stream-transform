FROM python:3.11-slim
WORKDIR /app
COPY app /app/app
RUN pip install fastapi uvicorn confluent-kafka pydantic boto3
ENV PORT=8080
CMD ["python","-m","app.main"]
