FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python \
    SPARK_LOCAL_IP=127.0.0.1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev,spark]"

COPY configs ./configs
COPY dags ./dags
COPY tests ./tests

ENV LAKEHOUSE_ENV=dev
CMD ["python", "-m", "lakehouse.jobs.run_bronze", "--env", "dev"]
