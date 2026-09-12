#!/usr/bin/env bash
# Runs once when the postgres container's data volume is first created.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO "$POSTGRES_USER";
EOSQL