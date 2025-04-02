FROM python:3.9-slim-bullseye

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

COPY requirements.txt ./
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -r appuser && chown -R appuser:appuser /app
USER appuser

COPY app/ ./app/
COPY .env .

EXPOSE 8000

# Use single process mode to avoid worker startup issues in Docker
CMD ["sanic", "app.main:app", "--host=0.0.0.0", "--port=8000", "--single-process", "--access-logs"]
