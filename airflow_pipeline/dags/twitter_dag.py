import sys
sys.path.append('airflow_pipeline')

from airflow.models import DAG
from datetime import datetime, timedelta
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago


with DAG(dag_id = 'TwitterDAG', start_date=days_ago(2), schedule_interval='@daily') as dag:
    TIME_STAMP = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIME_STAMP)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIME_STAMP)
    query = 'data science'

    to = TwitterOperator(file_path=join('datalake/twitter_datascience', 
                                        'extract_date={{ ds }}',
                                        'datascience_{{ ds_nodash }}.json'), 
                                        query=query, 
                                        start_time='{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}', 
                                        end_time='{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}', 
                                        task_id='twitter_datascience')