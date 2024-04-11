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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connection to API
CLIENT_ID = 
CLIENT_SECRET = 
USER_AGENT = 
api_inst = APIConnection(client_id=CLIENT_ID,
                         client_secret=CLIENT_SECRET,
                         user_agent=USER_AGENT)
reddit_client = api_inst.initialise_client()

data_inst = GetData(reddit_client=reddit_client,
                    firm_list_path='XXX',
                    subreddits=['wallstreetbets', 'investing', 'stocks', 'SecurityAnalysis', 'finance'])

try:
    with open("XXX\\last_run_time.txt", "r") as f:
        last_run_time_str = f.read().strip()
    last_run_time = datetime.datetime.strptime(last_run_time_str, '%Y-%m-%d %H:%M:%S')
except FileNotFoundError:
    # If the file doesn't exist, default to 24 hours ago
    last_run_time = datetime.datetime.now() - datetime.timedelta(days=1)

df = data_inst.get_comments(comment_target=100, last_run_time=last_run_time)
df = data_inst.clean_comments(df)

# Write data to db
database_manager = DatabaseManager(db_path="XXX")
database_manager.connect()
database_manager.create_table()
database_manager.insert_new_comments(df)
database_manager.close()

# Update last run time
last_run_time = datetime.datetime.now()
with open("XXX\\last_run_time.txt", "w") as f:
    f.write(last_run_time.strftime('%Y-%m-%d %H:%M:%S'))