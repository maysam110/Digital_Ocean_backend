FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose FastAPI (8000) and Prometheus metrics (9102)
EXPOSE 8000 9102

# Run with uvicorn — single process, async workers
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
