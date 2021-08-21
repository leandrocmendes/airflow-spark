from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from hooks.twitter_hook import TwitterHook
import json
from datetime import datetime

class TwitterOperator(BaseOperator):

    def __init__(
        self,  
        query,
        http_conn_id: str,
        start_time = None,
        end_time = None,
        *args, 
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query,
        self.http_conn_id = http_conn_id,
        self.start_time = start_time,
        self.end_time = end_time,

    def execute(self, context):
        print(self.start_time)
        hook = TwitterHook(
            query=''.join(self.query), 
            http_conn_id=''.join(self.http_conn_id),
            start_time=''.join(self.start_time),
            end_time=''.join(self.end_time),
        )

        for pg in hook.run():
            print(json.dumps(pg, indent=4, sort_keys=True))


if __name__ == "__main__":
     with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
         to = TwitterOperator(
             query="AluraOnline", 
             task_id= "test_run", 
             http_conn_id="twitter_default", 
             start_time="", 
             end_time="",
         )
         ti = TaskInstance(
             task=to,
             execution_date=datetime.now(),
         )
         to.execute(ti.get_template_context())