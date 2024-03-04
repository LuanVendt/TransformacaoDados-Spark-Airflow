import sys
sys.path.append('airflow_pipeline')

from airflow.models import DAG, BaseOperator, TaskInstance
from hook.twitter_hook import TwitterHook
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import json

class TwitterOperator(BaseOperator):

    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
         (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        end_time = self.end_time
        start_time = self.start_time
        query = self.query

        self.create_parent_folder()
        with open(self.file_path, 'w') as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():
                pg = json.loads(pg)
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write('\n')

if __name__ == '__main__':
    # montando url
        TIME_STAMP = "%Y-%m-%dT%H:%M:%S.00Z"

        end_time = datetime.now().strftime(TIME_STAMP)
        start_time = (datetime.now() + timedelta(-1)).date().strftime(TIME_STAMP)
        query = 'data science'

        with DAG(dag_id = 'TwitterTest', start_date=datetime.now()) as dag:
            to = TwitterOperator(file_path=join('datalake/twitter_datascience', 
                                                f'extract_date={datetime.now().date()}',
                                                f'datascience_{datetime.now().date().strftime("%Y%m%d")}.json'), 
                                                query=query, start_time=start_time, end_time=end_time, task_id='test_run')
            ti = TaskInstance(task=to)
            to.execute(ti.task_id)