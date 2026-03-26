# Use a lightweight official Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Set environment variables to optimize Python execution
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies required for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy ONLY requirements first (for Docker layer caching)
COPY requirements.txt .

# 2. Install dependencies (this heavy step gets cached unless requirements.txt changes)
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the rest of the application code
COPY . .

# Run the web service using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]