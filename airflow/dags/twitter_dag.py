from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
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
                "/home/leandro/Documents/workspace/datalake/bronze/",
                "twitter_aluraonline",
                "extract_date={{ ds }}",
                "AluraOnline_{{ ds_nodash }}.json"
        ),
        http_conn_id="twitter_default", 
        start_time="", 
        end_time="",
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=(
            "/home/leandro/Documents/workspace/spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            "/home/leandro/Documents/workspace/datalake/bronze/twitter_aluraonline/extract_date=2021-08-29",
            "--dest",
            "/home/leandro/Documents/workspace/datalake/silver/twitter_aluraonline",
            "--process-date",
            "{{ ds }}"
        ]
    )

