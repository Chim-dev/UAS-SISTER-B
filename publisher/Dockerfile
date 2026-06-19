# Image minimal untuk publisher/simulator (Python 3.11-slim, non-root user).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher.py .

RUN useradd --create-home --uid 10002 appuser
USER appuser

CMD ["python", "publisher.py"]
