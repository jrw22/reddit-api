import pandas as pd
from datetime import datetime, timedelta
import logging

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import torch
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance, PartOfSpeech
from umap import UMAP
from hdbscan import HDBSCAN
from sklearn.feature_extraction.text import CountVectorizer


def get_sentiment(df: pd.DataFrame, 
                  text_column: str):
    """Caluclate sentiment scores from Reddit comments"""
    # Initialise VADER
    sia = SentimentIntensityAnalyzer()
    # Apply VADER analysis on text column
    sent_df=df.copy()
    sent_df['sentiment_scores'] = sent_df[text_column].apply(lambda x: sia.polarity_scores(x))
    sent_df['compound'] = sent_df['sentiment_scores'].apply(lambda score_dict: score_dict['compound'])
    sent_df['sentiment'] = sent_df['compound'].apply(lambda c: 'POSITIVE' if c >= 0.05 else ('NEGATIVE' if c <= -0.05 else 'NEUTRAL'))
    
    return sent_df

def get_topics(df:pd.DataFrame, 
               text_column:str, 
               last_n_days:int=3, 
               embedding_model_path:str="C://Users//333866//Documents//dev//models//bge-large-en", 
               umap_n_neighbours:int=50, 
               umap_n_components:int=5,
               umap_min_dist:float=0,
               umap_metric:str='cosine',
               hdbscan_min_cluster_size:int=None,
               hdbscan_cluster_metric:str='euclidean',
               vectoriser_min_ngram:int=1,
               vectoriser_max_ngram:int=3):
    
    """
    BERTopic model for topic modelling of Reddit comments
    """
    
    # Filter for past n_days
    df['comment_date'] = pd.to_datetime(df['comment_date'])
    seven_days_ago = datetime.now() - timedelta(days=last_n_days) 
    recent_comments = df[df['comment_date'] >= seven_days_ago]
    recent_comments = df[text_column].to_list()

    # Create embeddings
    embedding_model = SentenceTransformer(embedding_model_path)
    embeddings = embedding_model.encode(recent_comments, show_progress_bar=True)

    # Reduce dimensionality
    umap_model = UMAP(n_neighbors=umap_n_neighbours, n_components=umap_n_components, min_dist=umap_min_dist, metric=umap_metric, random_state=42)

    # Cluster
    if hdbscan_min_cluster_size == None:
        hdbscan_min_cluster_size = int(len(df)/10)
    hdbscan_model = HDBSCAN(min_cluster_size=hdbscan_min_cluster_size, metric=hdbscan_cluster_metric, cluster_selection_method='eom', prediction_data=True)

    # Vectorise
    vectorizer_model = CountVectorizer(stop_words="english", min_df=2, ngram_range=(vectoriser_min_ngram, vectoriser_max_ngram))

    # Define representation models
    keybert_model = KeyBERTInspired()
    #pos_model = PartOfSpeech("en_core_web_sm")
    mmr_model = MaximalMarginalRelevance(diversity=0.3)
    representation_model = {
        "KeyBERT": keybert_model,
        "MMR": mmr_model
        #"POS": pos_model
    }

    # Build topic model
    topic_model = BERTopic(
    # Pipeline models
    embedding_model=embedding_model,
    umap_model=umap_model,
    hdbscan_model=hdbscan_model,
    vectorizer_model=vectorizer_model,
    representation_model=representation_model,
    # Hyperparameters
    top_n_words=10,
    verbose=True,
    calculate_probabilities=True
    )
    topics, probs = topic_model.fit_transform(recent_comments, embeddings)
    df = topic_model.get_topic_info()
    logging.info("Topic modelling complete")

    ##-- Formatting df --##
    df.drop('Representative_Docs', axis=1, inplace=True)
    # Placeholders for future development
    df['POS'] = ''
    df['OpenAI'] = ''
    # Add datestamp
    df['date'] = datetime.now().date()

    def list_to_string(x):
        if isinstance(x, list):
            return ', '.join(x)
        return x

    df = df.applymap(list_to_string)
    return df