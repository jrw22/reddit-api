import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def get_sentiment(df: pd.DataFrame, text_column: str):
    """Caluclate sentiment scores from Reddit comments"""
    # Initialise VADER
    sia = SentimentIntensityAnalyzer()
    # Apply VADER analysis on text column
    sent_df=df.copy()
    sent_df['sentiment_scores'] = sent_df[text_column].apply(lambda x: sia.polarity_scores(x))
    sent_df['compound'] = sent_df['sentiment_scores'].apply(lambda score_dict: score_dict['compound'])
    sent_df['sentiment'] = sent_df['compound'].apply(lambda c: 'POSITIVE' if c >= 0.05 else ('NEGATIVE' if c <= -0.05 else 'NEUTRAL'))
    
    return sent_df