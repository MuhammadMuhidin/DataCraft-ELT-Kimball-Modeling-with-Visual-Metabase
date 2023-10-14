#!/bin/bash
airflow db init
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py
airflow connections add 'connection_postgres' \
    --conn-type 'postgres' \
    --conn-host $POSTGRES_CONTAINER_NAME \
    --conn-schema $POSTGRES_CONTAINER_NAME \
    --conn-login $POSTGRES_USER \
    --conn-password $POSTGRES_PASSWORD \
    --conn-port '5432'
airflow webserver
