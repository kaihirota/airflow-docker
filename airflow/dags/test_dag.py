from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from nlp_operator import NLPOperator
from reddit_operator import RedditOperator

start_date = datetime.utcnow()

default_args = {
    'owner': 'kai',
    'start_date': start_date,
    'end_date': datetime(2030, 1, 1), #TODO
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name='test_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='testing Airflow',
          # schedule_interval='0 * * * *', TODO
          # max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

reddit_operator = RedditOperator(task_id='Get data from reddit',
                                 dag=dag,
                                 provide_context=True,
                                 postgres_conn_id='postgres_test',
                                 ticker='TSLA',
                                 sort_method='new',
                                 limit=5,
                                 # aws_credentials_id="aws_credentials",
                                 time_range='all')

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag,
#     provide_context=True,
#     aws_credentials_id="aws_credentials",
#     redshift_conn_id='redshift',
#     tables=["songplay", "users", "song", "artist", "time"]
# )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies

start_operator >> reddit_operator >> end_operator
