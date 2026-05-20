FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[spark,aws]"

COPY configs ./configs
COPY dags ./dags

ENV LAKEHOUSE_ENV=dev
CMD ["python", "-m", "lakehouse.jobs.run_full_pipeline", "--env", "dev"]
