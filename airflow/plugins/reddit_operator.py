from collections import defaultdict
import datetime as dt

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import INTEGER, TIMESTAMP, FLOAT
import praw

# from credentials import cred
cred = {
    'reddit': {
        "personal_use_script": "Et6PCJr09YeKzg",
        "secret": "Rw9Ix_g2uh7SWoqQuLW51a302yc",
        "username": "noname7743",
        "password": "from81@SV",
        "user_agent": "nlp-airflow"
    }
}

class RedditOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
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
                            client_id=cred['reddit']['personal_use_script'],
                            client_secret=cred['reddit']['secret'],
                            user_agent=cred['reddit']['user_agent'],
                            username=cred['reddit']['username'],
                            password=cred['reddit']['password'])
        psql = PostgresHook(postgres_conn_id=postgres_conn_id)

        try:
            self.engine = psql.get_sqlalchemy_engine()
        except:
            #TODO: delete later, this should be done when launching docker
            from cryptography.fernet import Fernet
            import os
            FERNET_KEY = Fernet.generate_key().decode()
            os.environ['AIRFLOW__CORE__FERNET_KEY'] = FERNET_KEY

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
            'post_id', 'created_utc', 'title', 'text', 'upvote', 'downvote', 'comments', 'subreddit', 'permalink',
            'author_id', 'author_name', 'author_has_verified_email'
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
        df.to_sql('tmp', self.engine, index=False,
                  if_exists='replace', dtype=dtypes_map)
