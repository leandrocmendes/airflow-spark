from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from hooks.twitter_hook import TwitterHook
import json
from datetime import datetime
from pathlib import Path

class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path",
        "start_time",
        "end_time"
    ]

    def __init__(
        self,  
        query,
        file_path,
        http_conn_id: str,
        start_time = None,
        end_time = None,
        *args, 
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query,
        self.file_path = file_path,
        self.http_conn_id = http_conn_id,
        self.start_time = start_time,
        self.end_time = end_time,

        print(self.start_time)
        print(self.end_time)

    def create_parent_folder(self):
        try:
            Path(Path(''.join(self.file_path)).parent).mkdir(parents=True, exist_ok=True)
        except AssertionError as error:
            print(error)

    def execute(self, context):
        hook = TwitterHook(
            query=''.join(self.query), 
            http_conn_id=''.join(self.http_conn_id),
            start_time=''.join(self.start_time),
            end_time=''.join(self.end_time),
        )

        try:
            self.create_parent_folder()
            
            with open(''.join(self.file_path), "w") as output_file:
                for pg in hook.run():
                    print(json.dump(pg, output_file,ensure_ascii=False))
                    output_file.write("\n")
        except AssertionError as error:
            print(error)