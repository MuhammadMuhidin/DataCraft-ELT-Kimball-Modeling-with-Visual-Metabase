include .env

help:
	@echo "## docker-build	- Build Docker Images (amd64) including its inter-container network."
	@echo "## postgres		- Run a Postgres container"
	@echo "## metabase		- Creating DB metabaseappdb and Run a Metabase container"
	@echo "## jupyter		- Spinup jupyter notebook for testing and validation purposes."
	@echo "## airflow		- Spinup airflow scheduler and webserver."

docker-build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker network create dibimbing-network
	@echo '__________________________________________________________'
	@docker build -t dibimbing-final/jupyter -f ./docker-final-project/Dockerfile.jupyter .
	@echo '__________________________________________________________'
	@docker build -t dibimbing-final/airflow -f ./docker-final-project/Dockerfile.airflow .
	@echo '==========================================================='

postgres:
	@docker-compose -f ./docker-final-project/docker-compose-postgres.yml --env-file .env up -d
	@echo '__________________________________________________________'
	@echo 'Postgres container created at port ${POSTGRES_PORT}...'
	@echo '__________________________________________________________'
	@echo 'Postgres Docker Host	: ${POSTGRES_CONTAINER_NAME}'
	@echo 'Postgres Account	: ${POSTGRES_USER}'
	@echo 'Postgres Password	: ${POSTGRES_PASSWORD}'
	@echo 'Postgres DB for Airflow : ${POSTGRES_DB}'
	@echo '==========================================================='

metabase:
	@echo '__________________________________________________________'
	@echo 'Creating DB metabaseappdb ...'
	@docker exec -it ${POSTGRES_CONTAINER_NAME} createdb -U ${POSTGRES_USER} metabaseappdb
	@echo 'Done created DB metabaseappdb'
	@echo '__________________________________________________________'
	@echo 'Creating Metabase Instance ...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker-final-project/docker-compose-metabase.yml --env-file .env up -d
	@echo '==========================================================='

jupyter:
	@echo '__________________________________________________________'
	@echo 'Creating Jupyter Notebook Cluster at http://localhost:${JUPYTER_PORT}...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker-final-project/docker-compose-jupyter.yml --env-file .env up -d
	@echo 'Created...'
	@echo 'Check log to details token...'
	@echo '==========================================================='

airflow:
	@echo '__________________________________________________________'
	@echo 'Creating Airflow Instance ...'
	@echo '__________________________________________________________'
	@docker-compose -f ./docker-final-project/docker-compose-airflow.yml --env-file .env up -d
	@echo '==========================================================='