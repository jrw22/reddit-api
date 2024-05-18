import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Establish a database connection."""
        self.conn = sqlite3.connect(self.db_path)
        logging.info("Database connection established.")

    def get_data(self, n_previous_days:int):
        """Retrieve data from comments table for past n days"""
        today = datetime.now()
        n_days_ago = today - timedelta(days=n_previous_days)
        query = """
        SELECT * FROM comments
        WHERE comment_date >= ?
        """
        df = pd.read_sql_query(query, self.conn, params=[n_days_ago])
        return df
    
    ### --- Raw data table --- ###

    def create_raw_table(self):
        """Create the comments table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            post_title TEXT,
            subreddit TEXT,
            comment_date TEXT,
            comment TEXT,
            matched_phrase TEXT,
            upvotes INTEGER
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)
        self.conn.commit()
        logging.info("Table 'comments' ready.")

    def get_existing_comment_ids(self, table):
        """Fetch existing comment IDs from the database."""
        query = f"SELECT comment_id FROM {table}"
        existing_ids = pd.read_sql(query, self.conn)
        return set(existing_ids['comment_id'])

    def insert_new_comments(self, df):
        """Insert new comments into the database, avoiding duplicates."""
        if self.conn is None:
            logging.error("Database connection not established.")
            return

        existing_comment_ids = self.get_existing_comment_ids(table='comments')
        df_new_comments = df[~df['comment_id'].isin(existing_comment_ids)]

        if not df_new_comments.empty:
            df_new_comments.to_sql(name='comments', 
                                   con=self.conn, 
                                   if_exists='append', 
                                   index=False)
            logging.info("New data inserted successfully into 'comments' table")
        else:
            logging.info("No new comments to insert into 'comments' table.")
    
    ### --- Sentiment table --- ###

    def create_sentiment_table(self):
        """Create the sentiment table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS sentiment (
            comment_id TEXT PRIMARY KEY,
            compound FLOAT,
            sentiment TEXT
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)
        self.conn.commit()
        logging.info("Table 'sentiment' ready.")
    
    def update_sentiment_table(self, df):
        """Insert new data into the sentiment table."""
        if self.conn is None:
            logging.error("Database connection not established.")
            return

        existing_comment_ids = self.get_existing_comment_ids(table='sentiment')
        df_new_comments = df[~df['comment_id'].isin(existing_comment_ids)]

        if not df_new_comments.empty:
            df_new_comments.to_sql(name='sentiment', 
                                   con=self.conn, 
                                   if_exists='append', 
                                   index=False)
            logging.info("New data inserted successfully into 'sentiment' table.")
        else:
            logging.info("No new data to insert into 'sentiment' table.")

    ### --- Topics table --- ###

    def get_latest_date(self, table:str):
        """Get latest date from the topics table"""
        query = f"SELECT date FROM {table} ORDER BY date DESC LIMIT 1"
        latest_date = pd.read_sql(query, self.conn)
        # Format to datetime object
        latest_date = latest_date['date'][0]
        return latest_date

    def create_topic_summaries_table(self):
        """Create the topic_summaries table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS topic_summaries (
            date TEXT,
            topic INTEGER,
            summary TEXT,
            keybert TEXT,
            mmr TEXT,
            size INTEGER
        );
        """
        cursor = self.conn.cursor()
        cursor.execute(create_table_sql)
        self.conn.commit()
        logging.info("Table 'topics' ready.")

    def update_topic_summaries_table(self, df):
        """Insert new data into the topic_summaries table."""
        if self.conn is None:
            logging.error("Database connection not established.")
            return
        
        df.to_sql(name='topic_summaries', 
                    con=self.conn, 
                    if_exists='append', 
                    index=False)
        logging.info("New data inserted successfully into 'topic_summaries' table.")


    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

