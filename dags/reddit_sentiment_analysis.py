from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from data_quality_operator import DataQualityOperator
from nlp_operator import NLPOperator
from reddit_operator import RedditOperator

default_args = {
    'owner': 'kai',
    'start_date': days_ago(1),
    # 'end_date': datetime(2030, 1, 1),
    'depends_on_past': False,
    # 'retries': 1,
    'email': ['kai@khirota.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
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
          schedule_interval=None)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

reddit_credentials = {
    "personal_use_script": os.environ['REDDIT_PERSONAL_USER_SCRIPT'],
    "secret": os.environ['REDDIT_SECRET'],
    "username": os.environ['REDDIT_USERNAME'],
    "password": os.environ['REDDIT_PW'],
    "user_agent": os.environ['REDDIT_USER_AGENT']
}

postgres_conn_id = os.environ['DB_AIRFLOW_CONN_ID']

"""
NOTE:
2 ways of passing arguments to DAGs

1. kwargs["ticker"]
kwargs["ticker"] available in __init__
pass '{{ dag_run.conf["ticker"] }}' into the operator like:

    reddit_operator = RedditOperator(task_id='Get_data_from_reddit',
                                     dag=dag,
                                     provide_context=True,
                                     cred=reddit_credentials,
                                     ticker = '{{ dag_run.conf["ticker"] }}',

2. context["dag_run"].conf["ticker"]
context["dag_run"].conf["ticker"] available in .execute() if  provide_context=True is passed when constructing Operator instance

"""

reddit_operator = RedditOperator(task_id='Get_data_from_reddit',
                                 dag=dag,
                                 provide_context=True,
                                 if_exists='replace',
                                 cred=reddit_credentials,
                                 ticker = '{{ dag_run.conf["ticker"] }}',
                                 postgres_conn_id=postgres_conn_id)

nlp_operator = NLPOperator(task_id='Run_NLP_pipeline',
                           dag=dag,
                           provide_context=True,
                           postgres_conn_id=postgres_conn_id)

run_quality_checks = DataQualityOperator(task_id='Run_data_quality_checks',
                                         dag=dag,
                                         provide_context=True,
                                         postgres_conn_id=postgres_conn_id)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Setting tasks dependencies
start_operator >> reddit_operator >> run_quality_checks >> nlp_operator >> end_operator
