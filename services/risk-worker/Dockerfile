FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY libs ./libs
COPY services/risk-worker ./services/risk-worker
COPY samples ./samples

RUN pip install --upgrade pip \
    && pip install .

CMD ["python", "services/risk-worker/main.py"]
