from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'kai',
    'start_date': days_ago(1),
    # 'end_date': datetime(2030, 1, 1),
    'catchup': False,
    'depends_on_past': False,
    'email': ['kai@khirota.co'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

dag_name='ETL_Sentiment'

dag = DAG(dag_name,
          default_args=default_args,
          description='Perform aggregation on reddit table and insert into sentiment table.',
          schedule_interval='@weekly',
          is_paused_upon_creation=True)

postgres_conn_id = os.environ['DB_AIRFLOW_CONN_ID']

# clear sentiment table first
sql = """TRUNCATE sentiment"""
clear_table = PostgresOperator(
                                task_id='Clear_tmp_table',
                                dag=dag,
                                postgres_conn_id=postgres_conn_id,
                                sql=sql)

# aggregate reddit table, insert into sentiment table
sql = """
    INSERT INTO sentiment
    SELECT stock AS ticker
        , DATE(created_utc) AS post_date
        , COUNT(DISTINCT post_id) AS posts
        , SUM(upvote) AS upvotes
        , SUM(downvote) AS downvotes
        , SUM(comments) AS comments
        , AVG(neg) AS negative
        , AVG(pos) AS positive
        , AVG(compound) AS compound
    FROM reddit
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """

sentiment_etl = PostgresOperator(
                                task_id='Sentiment_ETL',
                                dag=dag,
                                postgres_conn_id=postgres_conn_id,
                                sql=sql)

# Setting tasks dependencies
clear_table >> sentiment_etl
