import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def get_sentiment(df: pd.DataFrame, text_column: str):
    """Caluclate sentiment scores from Reddit comments"""
    # Initialise VADER
    sia = SentimentIntensityAnalyzer()
    # Apply VADER analysis on text column
    df['sentiment_scores'] = df[text_column].apply(lambda x: sia.polarity_scores(x))
    df['compound'] = df['sentiment_scores'].apply(lambda score_dict: score_dict['compound'])
    df['sentiment'] = df['compound'].apply(lambda c: 'POSITIVE' if c >= 0.05 else ('NEGATIVE' if c <= -0.05 else 'NEUTRAL'))
    
    return df