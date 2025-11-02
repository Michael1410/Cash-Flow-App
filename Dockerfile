FROM python:3.13-slim

# Set working directory
WORKDIR /code

# Install system deps required for building some Python packages (psycopg2)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
COPY requirements.txt /code/requirements.txt

# Upgrade pip and install Python deps
RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /code/requirements.txt

# Copy application code
COPY . /code

# Ensure logs are output immediately
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["uvicorn", "AWS.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
