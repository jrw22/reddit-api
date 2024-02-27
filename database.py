import sqlite3
from sqlite3 import Error
import pandas as pd
from datetime import datetime

class Database:
    def __init__(self, database: str):
        self.database = database
        self.conn = None 

    def create_connection(self):
        """Create a database connection and store it in the instance."""
        try:
            self.conn = sqlite3.connect(self.database)
            print("Connection to SQLite DB successful")
        except Error as e:
            print(f"The error '{e}' occurred")

    def close_connection(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print("Connection to SQLite DB closed")

    def create_table(self, create_table_sql):
        """Create a table from the create_table_sql statement."""
        try:
            c = self.conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(f"The error '{e}' occurred")

    def insert_data_from_df(self, df, table_name):
        """Insert new data from a DataFrame into the specified table."""
        try:
            df.to_sql(table_name, self.conn, if_exists='append', index=False)
            print(f"Data inserted successfully into {table_name}.")
        except Error as e:
            print(f"The error '{e}' occurred")

    def create_timestamp_table(self):
        """Create a table to store the script's last run time."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS script_metadata (
            id INTEGER PRIMARY KEY,
            last_run_time TEXT
        );
        """
        self.create_table(create_table_sql)

    def update_last_run_time(self, last_run_time):
        """Update the last run time in the script_metadata table."""
        update_sql = """
        INSERT INTO script_metadata (id, last_run_time)
        VALUES (1, ?)
        ON CONFLICT(id) DO UPDATE SET last_run_time = excluded.last_run_time;
        """
        try:
            c = self.conn.cursor()
            c.execute(update_sql, (last_run_time.isoformat(),))
            self.conn.commit()
        except Error as e:
            print(f"The error '{e}' occurred")

    def get_last_run_time_from_db(self):
        """Retrieve the last run time from the script_metadata table."""
        select_sql = "SELECT last_run_time FROM script_metadata WHERE id = 1;"
        try:
            c = self.conn.cursor()
            c.execute(select_sql)
            result = c.fetchone()
            if result:
                return datetime.fromisoformat(result[0])
            else:
                return datetime.min  # Or a default value
        except Error as e:
            print(f"The error '{e}' occurred")
            return datetime.min  # Or a default value



