# OJS2MongoDB project

## App versions
* Python 3.10.12
* Airflow 2.7.3
* apache-airflow-providers-mongo 3.4.0
* apache-airflow-providers-mysql 5.4.0
* MongoDB 7.0.4

## How to run

* Change `AIRFLOW_HOME` path in `run-airflow.sh` to a local folder
* sh `run-airflow.sh` - This  will launch Airflow in standalone mode
* Visit `http://localhost:8080/` on a web browser
* In `Admin > Connections`, create a MongoDB connection named `mongo`, a MySQL connection named `fiadmin` (with FiAdmin dump) and a local filesystem connection named `export_fiadmin`