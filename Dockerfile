# Use Ubuntu base image for ARM64 compatibility
FROM --platform=linux/amd64 mcr.microsoft.com/playwright/python:v1.52.0-jammy

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config.json .
COPY src/ ./src/

# Create data directory for output
RUN mkdir -p /app/data/backups

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Expose port for web UI
EXPOSE 8080

# Run the web server
CMD ["python", "src/web_server.py"]
