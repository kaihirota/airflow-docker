from collections import defaultdict
import datetime as dt
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id: str, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.psql = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = self.psql.get_sqlalchemy_engine()
        self.query = """SELECT * FROM tmp"""


    def execute(self, context):

        self.log.info("Begin data quality check...")

        self.log.info("Fetching data from tmp table")
        df = self.psql.get_pandas_df(sql=self.query, parameters=None)
        self.log.info(f"Fetched data from tmp table : {df.shape}")

        if len(df) == 0:
            self.log.error("Data quality check failed. 0 rows returned.")
        else:
            self.log.info("Data quality check passed.")
