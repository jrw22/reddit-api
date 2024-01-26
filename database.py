import sqlite3
from sqlite3 import Error

def main():
    database = r"reddit-sqlite.db"

    # Create a database connection
    conn = create_connection(database)
    if conn is not None:
        # Create comments table
        create_table(conn)
        # Insert comment data
        comment_data = (1, '2013-02-01', 'subreddit_name', 'author_name', 'This is a comment', 'HSBC', 100) # Amend this with my comment data
        insert_comment(conn, comment_data)

        # Close connection
        conn.close()
    else:
        print("Error! Cannot create the database connection.")

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    else:
        if conn:
            try:
                create_table(conn)
            except Error as e:
                print(e)

    return conn

def create_table(conn):
    """Create a table in the provided database connection"""

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS comments (
        id text PRIMARY KEY,
        date text NOT NULL,
        subreddit text NOT NULL,
        comment_author text NOT NULL,
        comment text NOT NULL,
        matched_phrase text NOT NULL,
        upvotes integer NOT NULL
    );
    """

    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)      

def insert_comment(conn, comment):
    """Insert a new comment into the comments table"""
    sql = ''' INSERT INTO comments(id, date, subreddit, comment_author, comment, matched_phrase, upvotes)
              VALUES(?,?,?,?,?, ?, ?) '''
    cur = conn.cursor()
    cur.execute(sql, comment)
    conn.commit()
    return cur.lastrowid      

if __name__ == '__main__':
    main()