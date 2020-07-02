from collections import defaultdict
import datetime as dt
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from sqlalchemy import INTEGER, TIMESTAMP, FLOAT
import spacy
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

class NLPOperator(BaseOperator):
    @apply_defaults
    def __init__(self, postgres_conn_id: str, *args, **kwargs):
        super(NLPOperator, self).__init__(*args, **kwargs)

        self.psql = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = self.psql.get_sqlalchemy_engine()
        self.query = """SELECT * FROM tmp"""

        try:
            self.analyzer = SentimentIntensityAnalyzer()
        except:
            nltk.download('vader_lexicon')
            self.analyzer = SentimentIntensityAnalyzer()

    def execute(self, context):
        nlp = spacy.load("en_core_web_sm")
        df = self.psql.get_pandas_df(sql=self.query, parameters=None)

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

        df['title_cleaned'] = df['title_cleaned'].apply(nlp_pipeline)
        df['text_cleaned'] = df['text_cleaned'].apply(nlp_pipeline)

        scores = defaultdict(list)
        for idx, row in df.iterrows():
            result = self.analyzer.polarity_scores(row['title'])
            scores['neg'] += result['neg'],
            scores['neut'] += result['neu'],
            scores['pos'] += result['pos'],
            scores['compound'] += result['compound'],

        df['neg'] = scores['neg']
        df['neut'] = scores['neut']
        df['pos'] = scores['pos']
        df['compound'] = scores['compound']

        df['created_utc'] = pd.to_datetime(df['created_utc'])
        df['saved_dt_utc'] = pd.to_datetime(df['saved_dt_utc'])
        df['upvote'] = df['upvote'].astype(int)
        df['downvote'] = df['downvote'].astype(int)
        df['comments'] = df['comments'].astype(int)

        dtypes_map = {
            'created_utc': TIMESTAMP,
            'saved_dt_utc': TIMESTAMP,
            'upvote': INTEGER,
            'downvote': INTEGER,
            'comments': INTEGER,
            'neg': FLOAT,
            'neut': FLOAT,
            'pos': FLOAT,
            'compound': FLOAT
        }
        df.to_sql('reddit', self.engine, index=False,
                  if_exists='append', dtype=dtypes_map)
