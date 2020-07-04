#!/usr/bin/sh

set -x
TRY_LOOP="20"

superset fab create-admin \
   --username $SUPERSET_USERNAME \
   --firstname $SUPERSET_FNAME \
   --lastname $SUPERSET_LNAME \
   --password $SUPERSET_PW \
   --email $SUPERSET_EMAIL

superset db upgrade
# superset load_examples
superset init

superset set-database-uri --database-name $DB_NAME --uri postgresql+psycopg2://${DB_USERNAME}:${DB_PW}@${DB_HOST}:${DB_PORT}/${DB_NAME}

gunicorn \
        --bind  "0.0.0.0:8088" \
        --access-logfile '-' \
        --error-logfile '-' \
        --workers 1 \
        --worker-class gthread \
        --threads 20 \
        --timeout 60 \
        --limit-request-line 0 \
        --limit-request-field_size 0 \
        "superset.app:create_app()"
