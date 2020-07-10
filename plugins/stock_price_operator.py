from collections import defaultdict
import datetime as dt
import time
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from alpha_vantage.timeseries import TimeSeries
import pandas as pd

class StockPriceOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id: str,
                 api_key: str,
                 *args,
                 **kwargs):

        super(StockPriceOperator, self).__init__(*args, **kwargs)
        self.psql = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = self.psql.get_sqlalchemy_engine()
        self.query = """SELECT ticker, name FROM watchlist"""
        # self.api_key = api_key
        self.ts = ts = TimeSeries(key=api_key,
                                  output_format='pandas',
                                  indexing_type='date')


    def execute(self, context):
        # ticker, name
        stocks = self.psql.get_pandas_df(sql=self.query, parameters=None)
        self.log.info(f"Loaded watchlist: {stocks['ticker'].unique()}")

        for idx, row in stocks.iterrows():
            ticker = row['ticker']

            df, info = self.ts.get_daily_adjusted(symbol=ticker, outputsize='full')

            if len(df) == 0:
                self.log.error(f"No data returned for stock: {ticker}")
                continue

            df.reset_index(inplace=True)
            cols = [
            'trade_date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'divident_amt', 'split_coef'
            ]
            df.columns = cols

            start_date = min(df['trade_date'].dt.strftime('%Y-%m-%d'))
            end_date = max(df['trade_date'].dt.strftime('%Y-%m-%d'))
            self.log.info(f"Fetched {len(df)} rows for {ticker}: {start_date} ~ {end_date}")

            df['volume'] = df['volume'].astype(int)
            df['ticker'] = ticker

            df.to_sql('stocks', self.engine, index=False, if_exists='append')

            time.sleep(15)
