# Python Worker image. No build stage needed — install deps and run.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# The Worker dials *out* to the Frontend; no inbound port to EXPOSE.
ENTRYPOINT ["python", "worker.py"]
