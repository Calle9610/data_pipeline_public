# Use Python 3.11 base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy project files
COPY src/ /app/src/
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Define the entry point
CMD ["python", "/app/src/main.py"]