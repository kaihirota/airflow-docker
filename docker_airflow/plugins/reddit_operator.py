from collections import defaultdict
import datetime as dt
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import INTEGER, TIMESTAMP, FLOAT
import praw

class RedditOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 cred,
                 postgres_conn_id: str,
                 ticker: str,
                 time_range: str,
                 sort_method='new',
                 limit=5,
                 *args,
                 **kwargs):
        """
        input:
        sort_method: relevance, hot, top, new, comments. (default new).
        time_range: all, day, hour, month, week, year (default: all).
        """

        super(RedditOperator, self).__init__(*args, **kwargs)
        self.ticker = ticker
        self.sort_method = sort_method
        self.limit = limit
        self.time_range = time_range

        self.reddit = praw.Reddit(
                            client_id=cred['personal_use_script'],
                            client_secret=cred['secret'],
                            user_agent=cred['user_agent'],
                            username=cred['username'],
                            password=cred['password'])

        psql = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = psql.get_sqlalchemy_engine()

    def execute(self, context):
        results = self.reddit.subreddit("all")
        query = f'{self.ticker} self:yes'
        data = []
        search_results = results.search(query,
                                        sort=self.sort_method,
                                        time_filter=self.time_range, limit=self.limit)

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

        cols = [
            'post_id', 'created_utc', 'title', 'text', 'upvote', 'downvote', 'comments', 'subreddit', 'permalink', 'author_name'
        ]

        df = pd.DataFrame(data, columns=cols)
        df['stock'] = self.ticker
        df['saved_dt_utc'] = dt.datetime.now().isoformat()
        df['created_utc'] = pd.to_datetime(df['created_utc'])
        df['saved_dt_utc'] = pd.to_datetime(df['saved_dt_utc'])
        df['upvote'] = df['upvote'].astype(int)
        df['downvote'] = df['downvote'].astype(int)
        df['comments'] = df['comments'].astype(int)
        df['title_cleaned'] = df['title'].str.replace(self.ticker, '')
        df['text_cleaned'] = df['text'].str.replace('(\n)+', ' ')

        dtypes_map = {
            'created_utc': TIMESTAMP,
            'saved_dt_utc': TIMESTAMP,
            'upvote': INTEGER,
            'downvote': INTEGER,
            'comments': INTEGER
        }

        # save to tmp table. replace content.
        df.to_sql('tmp', self.engine, index=False,
                  if_exists='replace', dtype=dtypes_map)
