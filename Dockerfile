FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock_payment.py .

ENV PYTHONUNBUFFERED=1
ENV KAFKA_BOOTSTRAP=host.docker.internal:9092

CMD ["python", "mock_payment.py"]
