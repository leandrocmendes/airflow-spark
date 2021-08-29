export AIRFLOW_HOME=$(pwd)/airflow

airflow db init
airflow webserver -D
airflow scheduler -D