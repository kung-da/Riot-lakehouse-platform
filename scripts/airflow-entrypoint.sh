#!/bin/sh
set -e

airflow db migrate
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
  --firstname Lakehouse \
  --lastname Admin \
  --role Admin \
  --email admin@example.com || true

airflow scheduler &
exec airflow webserver --port 8080
