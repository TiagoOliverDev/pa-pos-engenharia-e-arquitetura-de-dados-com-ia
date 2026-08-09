#!/bin/bash
set -euo pipefail

airflow db migrate

if airflow users list | grep -q "^${AIRFLOW_ADMIN_USER}[[:space:]]"; then
  echo "Airflow user ${AIRFLOW_ADMIN_USER} already exists."
  exit 0
fi

airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname "${AIRFLOW_ADMIN_FIRST_NAME}" \
  --lastname "${AIRFLOW_ADMIN_LAST_NAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}"

