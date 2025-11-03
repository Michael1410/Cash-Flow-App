FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY AWS/app/requirements.txt /app/requirements.txt

#
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /app/requirements.txt

COPY AWS/app/ /app/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]