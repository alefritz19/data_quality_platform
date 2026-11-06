FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY src /app/src
COPY dagster_app /app/dagster_app
COPY dashboard /app/dashboard
COPY config /app/config
COPY README.md ARCHITECTURE.md LICENSE /app/

RUN mkdir -p /app/data/inbox /app/data/raw /app/data/staging \
    /app/data/mart /app/data/export /app/data/rejected /app/logs

EXPOSE 8501 3000

CMD ["sh", "-c", "python -m src.bootstrap && streamlit run dashboard/app.py --server.port=${STREAMLIT_PORT:-8501} --server.address=0.0.0.0 --server.headless=true"]
