from collections import defaultdict
import datetime as dt
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import INTEGER, TIMESTAMP, FLOAT
import praw

class RedditBatchOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 cred,
                 postgres_conn_id: str,
                 if_exists: str,
                 *args,
                 **kwargs):
        """
            input:
                if_exists: ['fail', 'replace', 'append']
        """

        super(RedditBatchOperator, self).__init__(*args, **kwargs)

        self.reddit = praw.Reddit(
                            client_id=cred['personal_use_script'],
                            client_secret=cred['secret'],
                            user_agent=cred['user_agent'],
                            username=cred['username'],
                            password=cred['password'])

        self.if_exists = if_exists

        self.psql = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = self.psql.get_sqlalchemy_engine()
        self.query = """SELECT ticker, name FROM watchlist"""


    def execute(self, context):
        """
        input:
            sort_method: relevance, hot, top, new, comments. (default new).
            time_range: all, day, hour, month, week, year (default: all).
        """
        limit = 50
        sort_method = 'new'
        time_range = 'all'

        # ticker, name
        stocks = self.psql.get_pandas_df(sql=self.query, parameters=None)
        self.log.info(f"Loaded watchlist: {stocks['ticker'].unique()}")

        stocks_df = pd.DataFrame()

        for idx, row in stocks.iterrows():
            ticker = row['ticker']

            results = self.reddit.subreddit("all")
            query = f'{ticker} self:yes'
            data = []

            self.log.info(f"Querying Reddit: ticker={ticker} time_range={time_range} sort_method={sort_method} limit={limit}")

            search_results = results.search(query,
                                            sort=sort_method,
                                            time_filter=time_range,
                                            limit=limit)

            if search_results:
                self.log.info("Result returned from Reddit")
            else:
                self.log.error("Search failed")

            for p in search_results:
                if not p.stickied:
                    created_dt = dt.datetime.fromtimestamp(p.created_utc)
                    row = [
                        p.id,
                        created_dt.isoformat(),
                        p.title,
                        p.selftext,
                        p.ups,
                        p.downs,
                        p.num_comments,
                        p.subreddit.display_name,
                        p.permalink,
                        p.author.name
                    ]
                    data += row,

            self.log.info(f"Data has {len(data)} rows")

            cols = [
                'post_id', 'created_utc', 'title', 'text', 'upvote', 'downvote', 'comments', 'subreddit', 'permalink', 'author_name'
            ]

            df = pd.DataFrame(data, columns=cols)
            df['stock'] = ticker

            stocks_df = pd.concat([stocks_df, df])

        self.log.info(f"Total rows: {len(stocks_df)}")

        stocks_df['saved_dt_utc'] = dt.datetime.now().isoformat()
        stocks_df['created_utc'] = pd.to_datetime(stocks_df['created_utc'])
        stocks_df['saved_dt_utc'] = pd.to_datetime(stocks_df['saved_dt_utc'])
        stocks_df['upvote'] = stocks_df['upvote'].astype(int)
        stocks_df['downvote'] = stocks_df['downvote'].astype(int)
        stocks_df['comments'] = stocks_df['comments'].astype(int)
        stocks_df['title_cleaned'] = stocks_df['title'].str.replace(ticker, '')
        stocks_df['text_cleaned'] = stocks_df['text'].str.replace('(\n)+', ' ')

        dtypes_map = {
            'created_utc': TIMESTAMP,
            'saved_dt_utc': TIMESTAMP,
            'upvote': INTEGER,
            'downvote': INTEGER,
            'comments': INTEGER
        }

        self.log.info("Saving data to tmp table")

        # save to tmp table. replace content.
        stocks_df.to_sql('tmp', self.engine, index=False,
                  if_exists=self.if_exists, dtype=dtypes_map)
        self.log.info("Saved.")
