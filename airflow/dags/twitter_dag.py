from airflow.models.dag import DAG
from datetime import datetime
from pathlib import Path
from os.path import join
import sys

sys.path.insert(0,"/home/leandro/Documents/workspace/airflow/")

from plugins.operators.twitter_operator import TwitterOperator

with DAG(
    dag_id="twitter_dag",
    start_date=datetime.now(),
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path= join(
                "/home/leandro/Documents/workspace/airflow/datalake",
                "twitter_aluraonline",
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json"
        ),
        http_conn_id="twitter_default", 
        start_time="", 
        end_time="",
    )

