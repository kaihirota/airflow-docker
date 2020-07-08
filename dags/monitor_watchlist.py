from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from data_quality_operator import DataQualityOperator
from nlp_operator import NLPOperator
from reddit_batch_operator import RedditBatchOperator

default_args = {
    'owner': 'kai',
    'start_date': days_ago(1),
    # 'end_date': datetime(2030, 1, 1),
    'catchup': True,
    'depends_on_past': False,
    'email': ['kai@khirota.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

dag_name='Monitor_Watchlist'

dag = DAG(dag_name,
          default_args=default_args,
          description='For each stock in watchlist, Search and run sentiment analysis on Reddit headlines.',
          schedule_interval='@daily')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

postgres_conn_id = os.environ['DB_AIRFLOW_CONN_ID']

# clear tmp table first
sql = """TRUNCATE tmp"""
reset_tmp_table = PostgresOperator(task_id='Clear_tmp_table',
                                   dag=dag,
                                   sql=sql,
                                   postgres_conn_id=postgres_conn_id)

reddit_credentials = {
    "personal_use_script": os.environ['REDDIT_PERSONAL_USER_SCRIPT'],
    "secret": os.environ['REDDIT_SECRET'],
    "username": os.environ['REDDIT_USERNAME'],
    "password": os.environ['REDDIT_PW'],
    "user_agent": os.environ['REDDIT_USER_AGENT']
}

reddit_batch_nlp_operator = RedditBatchOperator(
                                        task_id='Get_data_from_reddit_batch',
                                        dag=dag,
                                        if_exists='append',
                                        cred=reddit_credentials,
                                        postgres_conn_id=postgres_conn_id)

run_quality_checks = DataQualityOperator(task_id='Run_data_quality_checks',
                                         dag=dag,
                                         provide_context=True,
                                         postgres_conn_id=postgres_conn_id)

nlp_operator = NLPOperator(task_id='Run_NLP_pipeline',
                           dag=dag,
                           provide_context=True,
                           postgres_conn_id=postgres_conn_id)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Setting tasks dependencies
start_operator >> reset_tmp_table >> reddit_batch_nlp_operator >> run_quality_checks >> nlp_operator >> end_operator
