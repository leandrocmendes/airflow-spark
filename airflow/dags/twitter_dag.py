from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from os.path import join
from pathlib import Path
import sys

sys.path.insert(0,"/home/leandro/Documents/workspace/airflow/")

from plugins.operators.twitter_operator import TwitterOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

timestamp_format = "%Y-%m-%dT%H:%M:%S.00Z"


base_folder = join(
    str(Path("~/Documents").expanduser()),
    "workspace/datalake/{stage}/twitter_aluraonline/{partition}"
)

partition_folder = "extract_date={{ ds }}"

with DAG(
    dag_id="twitter_dag",
    default_args=default_args,
    schedule_interval="0 12 * * *",
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="afeoficial",
        file_path= join(
            base_folder.format(stage="bronze", partition=partition_folder),
            "AluraOnline_{{ ds_nodash }}.json"
        ),
        http_conn_id="twitter_default", 
        start_time=(
            "{{" 
            f"execution_date.strftime('{ timestamp_format }')"
            "}}"
        ), 
        end_time=(
            "{{" 
            f"next_execution_date.strftime('{ timestamp_format }')"
            "}}"
        ),
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=join(
            str(Path(__file__).parents[2]),
            "spark/transformation.py"
        ),
        name="twitter_transformation",
        application_args=[
            "--src",
            base_folder.format(stage="bronze", partition=partition_folder),
            "--dest",
            base_folder.format(stage="silver", partition=""),
            "--process-date",
            "{{ ds }}"
        ]
    )

twitter_operator >> twitter_transform
