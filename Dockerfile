# Use lightweight Python base image
FROM python:3.9-slim-buster

# Set working directory
WORKDIR /app

# Copy everything into the container
COPY . /app

# Install system dependencies (awscli + cleanup)
RUN apt-get update -y && apt-get install -y awscli && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command
CMD ["python", "app.py"]
