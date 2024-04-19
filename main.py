# Standard imports
import logging
import datetime
import pandas as pd

# Related 3rd party imports
import praw 

# Local imports
from api import APIConnection
from get_data import GetData
from database import DatabaseManager
import analytics as an

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

##-- Connect to API --##
CLIENT_ID = 
CLIENT_SECRET = 
USER_AGENT = 
api_inst = APIConnection(client_id=CLIENT_ID,
                         client_secret=CLIENT_SECRET,
                         user_agent=USER_AGENT)
reddit_client = api_inst.initialise_client()

##-- Extract comment data --##
data_inst = GetData(reddit_client=reddit_client,
                    firm_list_path=,
                    subreddits=['wallstreetbets', 'investing', 'stocks', 'SecurityAnalysis', 'finance'])

try:
    with open(, "r") as f:
        last_run_time_str = f.read().strip()
    last_run_time = datetime.datetime.strptime(last_run_time_str, '%Y-%m-%d %H:%M:%S')
except FileNotFoundError:
    # If the file doesn't exist, default to 24 hours ago
    last_run_time = datetime.datetime.now() - datetime.timedelta(days=1)

df = data_inst.get_comments(comment_target=100, last_run_time=last_run_time)
df = data_inst.clean_comments(df)

##-- Analysis --##
# Sentiment analysis
sent_df = an.get_sentiment(df, 'comment')
sent_df = sent_df[['comment_id', 'compound', 'sentiment']]

##-- Write data to db --##
# Connect to db
database_manager = DatabaseManager(db_path=)
database_manager.connect()
# Add raw data to db
database_manager.create_raw_table()
database_manager.insert_new_comments(df)
# Add sentiment data to db
database_manager.create_sentiment_table()
database_manager.update_sentiment_table(sent_df)
# Close connection to db
database_manager.close()

##-- Update last run time --##
last_run_time = datetime.datetime.now()
with open(, "w")  as f:
    f.write(last_run_time.strftime('%Y-%m-%d %H:%M:%S'))