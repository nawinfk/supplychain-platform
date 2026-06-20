FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY libs ./libs
COPY services/vessel-consumer-worker ./services/vessel-consumer-worker
COPY samples ./samples

RUN pip install --upgrade pip \
    && pip install . \
    && chown -R 10001:10001 /app

USER 10001:10001

CMD ["python", "services/vessel-consumer-worker/main.py"]
