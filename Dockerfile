# Use Python 3.11 base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the necessary project files
COPY requirements.txt /app/
COPY src/ /app/src/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port (optional, if your app needs an external port)
EXPOSE 5000

# Define the entry point
CMD ["python", "/app/src/main.py"]
