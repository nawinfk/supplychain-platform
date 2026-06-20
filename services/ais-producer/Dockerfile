FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \\
    PYTHONUNBUFFERED=1 \\
    PIP_NO_CACHE_DIR=1

WORKDIR /app
RUN addgroup --system app && adduser --system --ingroup app app
COPY pyproject.toml ./
COPY libs ./libs
RUN pip install --upgrade pip && pip install .
COPY services/ais-producer/main.py ./service/main.py
RUN chown -R app:app /app
USER app
HEALTHCHECK --interval=30s --timeout=3s --retries=3 CMD python -c "import os; os.kill(1, 0)"
CMD ["python", "service/main.py"]
