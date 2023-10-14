# Dibimbing, Data Engineering Bootcamp
# Final Project

---
```
"## docker-build	- Build Docker Images (amd64) including its inter-container network."
"## postgres		- Run a Postgres container"
"## metabase		- Creating DB metabaseappdb and Run a Metabase container"
"## jupyter		- Spinup jupyter notebook for testing and validation purposes."
"## airflow		- Spinup airflow scheduler and webserver."
```
---

Need edit here with your:
- AIRFLOW__SMTP__SMTP_PASSWORD=<YOUR APP PASSWORD HERE> in docker-compose-airflow.yml
EMAIL_ADDR in .env

Need create connection in airflow for send notifications telegram & slack:
- telegram_default
- slack_default

![flow](flow.svg)
![email](email.png)
![attachment](attachment.png)
![telegram](telegram.png)
![slack](slack.png)