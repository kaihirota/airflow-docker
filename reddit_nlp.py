from collections import defaultdict
import datetime as dt

import pandas as pd
from sqlalchemy import create_engine, INTEGER, TIMESTAMP, FLOAT
import praw
import spacy
from nltk.sentiment.vader import SentimentIntensityAnalyzer

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

class RedditConnector:
    def __init__(self):
        self.reddit = praw.Reddit(
                            client_id=cred['reddit']['personal_use_script'],
                            client_secret=cred['reddit']['secret'],
                            user_agent=cred['reddit']['user_agent'],
                            username=cred['reddit']['username'],
                            password=cred['reddit']['password'])

    def query_reddit(self, ticker: str, sort_method='new', limit=5, time_range='all'):
        """
        input:
            sort_method: relevance, hot, top, new, comments. (default new).
            time_range: all, day, hour, month, week, year (default: all).
        """
        results = self.reddit.subreddit("all")
        query = f'{ticker} self:yes'
        data = []
        search_results = results.search(query, sort=sort_method,
                                        time_filter=time_range, limit=limit)

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
        df['stock'] = ticker
        df['saved_dt_utc'] = dt.datetime.now().isoformat()
        df['created_utc'] = pd.to_datetime(df['created_utc'])
        df['saved_dt_utc'] = pd.to_datetime(df['saved_dt_utc'])
        df['upvote'] = df['upvote'].astype(int)
        df['downvote'] = df['downvote'].astype(int)
        df['comments'] = df['comments'].astype(int)
        df['title_cleaned'] = df['title'].str.replace(ticker, '')
        df['text_cleaned'] = df['text'].str.replace('(\n)+', ' ')
        self.df = df


    def sentiment_analysis(self):
        nlp = spacy.load("en_core_web_sm")

        def nlp_pipeline(txt):
            """
            1. remove stop words, punctuations
            2. lemmatize if possible (past tense -> present tense, etc)
            3. cast to lower case
            """
            doc = nlp(txt)
            newdoc = []

            for token in doc:

                if token.is_stop or token.is_punct or not token.is_alpha:
                    pass
                elif token.text != token.lemma_ and token.lemma_ != '-PRON-':
                    newdoc += token.lemma_,
                else:
                    newdoc += token.lower_,

            return " ".join(newdoc)

        self.df['title_cleaned'] = self.df['title_cleaned'].apply(nlp_pipeline)
        self.df['text_cleaned'] = self.df['text_cleaned'].apply(nlp_pipeline)

        scores = defaultdict(list)
        for idx, row in df.iterrows():
            result = analyzer.polarity_scores(row['title'])
            scores['neg'] += result['neg'],
            scores['neut'] += result['neu'],
            scores['pos'] += result['pos'],
            scores['compound'] += result['compound'],

        self.df['neg'] = scores['neg']
        self.df['neut'] = scores['neut']
        self.df['pos'] = scores['pos']
        self.df['compound'] = scores['compound']
