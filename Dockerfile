# syntax=docker/dockerfile:1.7
FROM python:3.11-slim AS builder
ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip wheel --wheel-dir /wheels ".[stream]"

FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN useradd -m -u 10001 pfe
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels "pfe[stream]" && rm -rf /wheels
USER pfe
HEALTHCHECK --interval=60s --timeout=5s CMD pgrep -f pfe-stream >/dev/null || exit 1
ENTRYPOINT ["pfe-stream"]
CMD ["news"]

FROM apache/airflow:2.9.3-python3.11
RUN pip install --no-cache-dir "apache-airflow-providers-databricks==6.7.0" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
