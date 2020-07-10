from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.postgres_operator import PostgresOperator
from data_quality_operator import DataQualityOperator
from stock_price_operator import StockPriceOperator

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


dag_name='Fetch_OHLC_Watchlist'
api_key = os.environ['ALPHA_VANTAGE_API']
postgres_conn_id = os.environ['DB_AIRFLOW_CONN_ID']

dag = DAG(dag_name,
          default_args=default_args,
          description='Get historical daily OHLC data for each stock in watchlist.',
          schedule_interval='@weekly',
          is_paused_upon_creation=False)

# clear tmp table first
sql = """TRUNCATE stocks"""
reset_tmp_table = PostgresOperator(
                                task_id='Clear_tmp_table',
                                dag=dag,
                                postgres_conn_id=postgres_conn_id,
                                sql=sql)

get_financial_data = StockPriceOperator(
                                task_id='Get_financial_data_for_watchlist',
                                dag=dag,
                                postgres_conn_id=postgres_conn_id,
                                api_key=api_key)

run_quality_checks = DataQualityOperator(
                                task_id='Run_data_quality_checks',
                                provide_context=True,
                                dag=dag,
                                postgres_conn_id=postgres_conn_id)

# Setting tasks dependencies
reset_tmp_table >> get_financial_data >> run_quality_checks
