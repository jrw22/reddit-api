# Standard imports
import logging
import datetime
import pandas as pd

# Related 3rd party imports
import praw 
from datetime import datetime

# Local imports
from api import APIConnection
from get_data import GetData
from database import DatabaseManager
import analytics as an

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

### --- CONNECT TO API --- ###

CLIENT_ID= ''
CLIENT_SECRET= ''
USER_AGENT= ''
api_connection = APIConnection(CLIENT_ID, CLIENT_SECRET, USER_AGENT)

### --- EXTRACT COMMENT DATA --- ###

# Initialise data collection class
data_inst = GetData(api_connection=api_connection,
                    firm_list_path="",
                    subreddits=['wallstreetbets', 'investing', 'stocks', 'SecurityAnalysis', 'finance'])

# Get last run time - only want to check comments since the last run time 
try:
    with open("", "r") as f:
        last_run_time_str = f.read().strip()
    last_run_time = datetime.strptime(last_run_time_str, '%Y-%m-%d %H:%M:%S')
except FileNotFoundError:
    # If the file doesn't exist, default to 24 hours ago
    last_run_time = datetime.now() - datetime.timedelta(days=1)
    logging.error(f"last_run_time.txt not found.")

# Get comment data
try:
    df = data_inst.get_comments(comment_target=100, last_run_time=last_run_time)
except Exception as e:
    logging.error(f"Error retrieving comment data: {e}")
try:
    df = data_inst.clean_comments(df)
except Exception as e:
    logging.error(f"Error cleaning comment data: {e}")

# Update last run time
last_run_time = datetime.now()
try:
    with open("", "w")  as f:
        f.write(last_run_time.strftime('%Y-%m-%d %H:%M:%S'))
except FileNotFoundError:
    logging.error(f"last_run_time.txt not found.")


### --- WRITE DATA TO DATABASE --- ###

# Connect to db
try:
    database_manager = DatabaseManager(db_path="")
    database_manager.connect()
except Exception as e:
    logging.error(f"Error connecting to database: {e}")
# Add raw data to db
try:
    database_manager.create_raw_table()
    database_manager.insert_new_comments(df)
except Exception as e:
    logging.error(f"Error adding new data to comments table: {e}")

# Log total API calls
logging.info(f"Total API calls made: {api_connection.get_total_calls()}")

### --- ANALYSIS --- ###
    
## Sentiment analysis
try:
    sent_df = an.get_sentiment(df, 'comment')
    sent_df = sent_df[['comment_id', 'compound', 'sentiment']]
except Exception as e:
    logging.error(f"Error conducting sentiment analysis: {e}")

# Add sentiment data to db
try:
    database_manager.create_sentiment_table()
    database_manager.update_sentiment_table(sent_df)
except Exception as e:
    logging.error(f"Error adding new data to sentiment table: {e}")

## Topic modelling and Summarisation
    
# Check if it's been ran today - compute intensive so only run once a day
latest_date = database_manager.get_latest_date(table='topics')
latest_date = datetime.strptime(latest_date, '%Y-%m-%d').date()
if datetime.today().date() != latest_date:
    
    # Get last 7 days comment data from db
    try:
        recent_df = database_manager.get_data(n_previous_days=7)
        comments = recent_df['comment'].to_list()
    except:
        logging.error(f"Error getting most recent data.")
    
    # Topic modelling
    try:
        topics, topics_info = an.get_topics(comments, hdbscan_min_cluster_size=int(len(recent_df)/20))
    except Exception as e:
        logging.error(f"Error conducting topic modelling: {e}")

    # Topic summarisation
    try:
        topics_summary_df = an.topic_summarisation(comments, topics, topics_info)
    except Exception as e:
        logging.error(f"Error summarising topic clusters: {e}")

    # Add topic summarisation to db
    try:
        database_manager.create_topic_summaries_table()
        database_manager.update_topic_summaries_table(topics_summary_df)
    except Exception as e:
        logging.error(f"Error adding new data to topics_summary table: {e}")

else:
    logging.info("Topic modelling already conducted today.")

### --- CLEAN UP --- ###

# Close connection to db
database_manager.close()

