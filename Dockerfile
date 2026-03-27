
FROM python:3.11-slim

WORKDIR /app

# System deps for some packages (psycopg2 may need build deps on slim)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy consumer script and optionally any certs
COPY ../consumercopy.py ./consumercopy.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "consumercopy.py"]