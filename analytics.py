import pandas as pd
from datetime import datetime, timedelta
import logging
from collections import defaultdict

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import torch
from sentence_transformers import SentenceTransformer
from transformers import BartTokenizer, BartForConditionalGeneration
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

def get_topics(comments: list,
               embedding_model_path:str="bge-large-en", 
               umap_n_neighbours:int=50, 
               umap_n_components:int=5,
               umap_min_dist:float=0,
               umap_metric:str='cosine',
               hdbscan_min_cluster_size:int=10,
               hdbscan_cluster_metric:str='euclidean',
               vectoriser_min_ngram:int=1,
               vectoriser_max_ngram:int=3)->pd.DataFrame:
    
    """
    BERTopic model for topic modelling of Reddit comments.

     Params:
    -------
    * comments `list`: text to topic model. 
    * embedding_model_path `str`: filepath to embedding model.
    * umap_n_neighbours `int`: 
    * umap n_components `int`:
    * umap_min_dist `float`:
    * umap_metric `str`:
    * hdbscan_min_cluster_size `int`:
    * hbdscan_cluster_metric `str`:
    * vectoriser_min_ngram `int`:
    * vectroiser_max_ngram `int`:
    
    Returns:
    --------
    df `pd.DataFrame`: a DataFrame of topic modelling representations. 
    topics:
    topics_info:
    """
    # Create embeddings
    embedding_model = SentenceTransformer(embedding_model_path)
    embeddings = embedding_model.encode(comments, show_progress_bar=True)

    # Reduce dimensionality
    umap_model = UMAP(n_neighbors=umap_n_neighbours, n_components=umap_n_components, min_dist=umap_min_dist, metric=umap_metric, random_state=42)

    # Cluster
    hdbscan_model = HDBSCAN(min_cluster_size=hdbscan_min_cluster_size, metric=hdbscan_cluster_metric, cluster_selection_method='eom', prediction_data=True)

    # Vectorise
    vectorizer_model = CountVectorizer(stop_words="english", min_df=1, ngram_range=(vectoriser_min_ngram, vectoriser_max_ngram))

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
    topics, probs = topic_model.fit_transform(comments, embeddings)
    topic_info = topic_model.get_topic_info()
    # df = topic_model.get_topic_info()
    logging.info("Topic modelling complete")

    topic_info['date'] = datetime.now().date()

    return topics, topic_info

def summarise(
        text_to_summarise:str,
        model_path:str="bart-cnn-large", 
        input_chunk_max_length:int=1024,
        return_tensors:str='pt',
        truncation:bool=True,
        padding:str='max_length',
        output_max_length:int=150,
        output_min_length:int=40,
        length_penalty:float=2.0,
        early_stopping:bool=True,
        num_beams:int=1
)->pd.DataFrame:
    """
    Function to summarise text using BART.

    Params:
    -------
    * text_to_summarise `str`: text to summarise.
    * model_path `str`: path to the pre-trained BART model.
    * input_chunk_max_length `int`: maximum length of input tokens.
    * return_tensors `str`: format of the return tensors. Default: 'pt' for PyTorch tensors.
    * truncation `bool`: whether to truncate texts longer than `input_max_length`.
    * padding `str`: how to handle texts shorter than `input_max_length`.
    * output_max_length `int`: maximum length of the summary output.
    * output_min_length `int`: minimum length of the summary output.
    * length_penalty `float`: penalty for a summary that is too short or too long.
    * early_stopping `bool`: whether to stop once the model is sure about the output.
    * num_beams `int`: number of beams for beam search.
    * no_repeat_ngram_size `int`: minimum size of ngrams to not be repeated in the summary.
    * top_k:
    * top_p:
    
    Returns:
    --------
    summary_df `pd.DataFrame`: a DataFrame with a summary and date field. 
    """
    summariser = BartForConditionalGeneration.from_pretrained(model_path)
    tokeniser = BartTokenizer.from_pretrained(model_path)

    # Concatenate all text into a large string and split into chunks
    full_text = text_to_summarise
    chunks = [full_text[i:i + input_chunk_max_length] for i in range(0, len(full_text), input_chunk_max_length)]

    chunk_summaries = []
    for chunk in chunks:
        inputs = tokeniser(chunk, 
                        max_length=input_chunk_max_length, 
                        return_tensors=return_tensors, 
                        truncation=truncation, 
                        padding=padding)

        summary_ids = summariser.generate(inputs['input_ids'], 
                                    attention_mask=inputs['attention_mask'],
                                        max_length=output_max_length, 
                                        min_length=output_min_length,
                                        length_penalty=length_penalty,
                                        early_stopping=early_stopping,
                                        num_beams=num_beams,
                                        no_repeat_ngram_size=3,
                                        top_k=50,
                                        top_p=0.95
                                        )
        chunk_summary = tokeniser.decode(summary_ids[0], skip_special_tokens=True)
        chunk_summaries.append(chunk_summary)

    final_text_to_summarise = " ".join(chunk_summaries)
    final_inputs = tokeniser(final_text_to_summarise, 
                        max_length=input_chunk_max_length, 
                        return_tensors=return_tensors, 
                        truncation=truncation, 
                        padding=padding)
    final_summary_ids = summariser.generate(final_inputs['input_ids'], 
                                    attention_mask=inputs['attention_mask'],
                                        max_length=output_max_length, 
                                        min_length=output_min_length,
                                        length_penalty=length_penalty,
                                        early_stopping=early_stopping,
                                        num_beams=num_beams
                                        )
    final_summary = tokeniser.decode(final_summary_ids[0], skip_special_tokens=True)

    return final_summary

def topic_summarisation(comments: list, topics, topic_info)->pd.DataFrame:
    """This function combines the BERTopic modelling outputs with a 
    summarisation function to summarise clusters of text. 
    """

    # Initialise variables
    topic_comments = defaultdict(list)
    topic_summaries = []
    todays_date = datetime.now().date()

    # Create dictionary of comments by topic
    for comment, topic in zip(comments, topics):
        topic_comments[topic].append(comment)

    for topic_id, comments in topic_comments.items():
        if topic_id == -1: # skip outlier topic
            continue
        # Combine comments into a string
        combined_comments = ' '.join(comments)
        # Summarise
        summary = summarise(combined_comments)
        # Extract keywords
        keybert_list = topic_info[topic_info['Topic'] == topic_id]['KeyBERT'].values[0]
        keybert = ', '.join(keybert_list)
        mmr_list = topic_info[topic_info['Topic'] == topic_id]['MMR'].values[0]
        mmr = ', '.join(mmr_list)
        # Store summary for current topic
        topic_summaries.append({
            "Date": todays_date,
            "Topic": topic_id,
            "Summary": summary,
            "KeyBERT": keybert,
            "MMR": mmr,
            "Size": len(comments)
        })
        logging.info(f"Summary completed for topic cluster: {topic_id}")

    df_summaries = pd.DataFrame(topic_summaries)
    logging.info("Topics summarisation completed.")

    return df_summaries

    

