#!/usr/bin/sh

set -x
TRY_LOOP="20"

# Load DAGs examples
AIRFLOW__CORE__LOAD_EXAMPLES=False

# Global defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

export AIRFLOW_HOME \
       AIRFLOW__CORE__EXECUTOR \
       AIRFLOW__CORE__FERNET_KEY \
       AIRFLOW__CORE__LOAD_EXAMPLES

export AIRFLOW__SMTP__SMTP_PASSWORD=${SMTP_PASSWORD}

# export AIRFLOW_CONN_POSTGRES_TEST=postgresql://${DB_USERNAME}:${DB_PW}@${DB_HOST}:${DB_PORT}/${DB_NAME}

case "$1" in
  webserver)
    airflow initdb

    airflow create_user --role Admin --username ${ADMIN_USERNAME} -p ${ADMIN_PW} --email ${ADMIN_EMAIL} -f ${ADMIN_FNAME} -l ${ADMIN_LNAME}

    ## this method, unlike setting AIRFLOW_CONN_x environment variable, shows up in Airflow UI. however it must be executed after airflow initdb

    # s3 logging
    airflow connections -a --conn_id ${S3_CONN_ID} --conn_uri "s3://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@${S3_BUCKET}/logs"

    # main db
    airflow connections -a --conn_id ${DB_AIRFLOW_CONN_ID} --conn_uri "postgresql://${DB_USERNAME}:${DB_PW}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # With the "Local" and "Sequential" executors it should all run in one container.
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    # Give the webserver time to run initdb.
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac
