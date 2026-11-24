# Production Dockerfile for Preprint PDF Streaming API
FROM python:3.11-slim

# Set environment flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8000 \
    WORKERS=4

WORKDIR /app

# Install system dependencies (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY main.py pdf_resolver.py test_client.py README.md ./

# Expose API port
EXPOSE 8000

# Healthcheck probe
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/ || exit 1

# Start Uvicorn with multi-worker support for production
CMD ["sh", "-c", "uvicorn main:app --host $HOST --port $PORT --workers $WORKERS"]
