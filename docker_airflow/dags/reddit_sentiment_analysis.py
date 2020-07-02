from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from reddit_operator import RedditOperator
from nlp_operator import NLPOperator

default_args = {
    'owner': 'kai',
    'start_date': days_ago(1),
    # 'end_date': datetime(2030, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'max_active_runs': 1
}

"""
schedule_interval
timedelta(minutes=5)
None	Don’t schedule, use for exclusively “externally triggered” DAGs
@once	Schedule once and only once
@hourly	Run once an hour at the beginning of the hour
@daily	Run once a day at midnight
@weekly	Run once a week at midnight on Sunday morning
@monthly	Run once a month at midnight of the first day of the month
@yearly	Run once a year at midnight of January 1
"""

dag_name='Reddit_Sentiment_Analysis'

dag = DAG(dag_name,
          default_args=default_args,
          description='Search Reddit headlines about specified stock; run sentiment analysis.',
          schedule_interval='@once')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

reddit_credentials = {
    "personal_use_script": os.environ['REDDIT_PERSONAL_USER_SCRIPT'],
    "secret": os.environ['REDDIT_SECRET'],
    "username": os.environ['REDDIT_USERNAME'],
    "password": os.environ['REDDIT_PW'],
    "user_agent": os.environ['REDDIT_USER_AGENT']
}

postgres_conn_id = os.environ['DB_AIRFLOW_CONN_ID']
ticker = 'TSLA'
sort_method = 'new'
limit = 100
time_range = 'all'

reddit_operator = RedditOperator(task_id='Get_data_from_reddit',
                                 cred=reddit_credentials,
                                 dag=dag,
                                 provide_context=True,
                                 postgres_conn_id=postgres_conn_id,
                                 ticker=ticker,
                                 sort_method=sort_method,
                                 limit=limit,
                                 time_range=time_range)

nlp_operator = NLPOperator(task_id='Run_NLP_pipeline',
                           dag=dag,
                           provide_context=True,
                           postgres_conn_id=postgres_conn_id)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     provide_context=True,
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id='redshift',
#     tables=["songplay", "users", "song", "artist", "time"]
# )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Setting tasks dependencies
start_operator >> reddit_operator >> nlp_operator >> end_operator
