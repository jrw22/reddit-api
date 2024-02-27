from api import APIConnection
from get_data import GetData
from database import Database

import logging
from datetime import datetime
import pandas as pd

def main():
    
    ### SET-UP ### 
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    ### API CONNECTION ###
    CLIENT_ID =
    CLIENT_SECRET = 
    USER_AGENT = 

    api_inst = APIConnection(client_id=CLIENT_ID,
                         client_secret=CLIENT_SECRET,
                         user_agent=USER_AGENT)

    reddit_client = api_inst.initialise_client()


    ### GET LATEST TIMESTAMP ###
    db = Database("reddit-sqlite.db")
    db.create_connection()

    # Ensure the timestamp table exists
    db.create_timestamp_table()

    # Retrieve the last run time
    last_run_time = db.get_last_run_time_from_db()
    print(f"Last run time: {last_run_time}")

    
    ### GET DATA
    ticker_patterns = [r'\bBank\s+of\s+America\b', r'\bBAC\b',
                    r'\bBank\s+of\s+England\b', r'\bBoE\b',  # Central bank, not publicly traded
                    r'\bBarclays\b', r'\bBCS\b',
                    r'\bCiti\b', r'\bCitigroup\b', r'\bC\b',
                    r'\bCoutts\b',  # Part of NatWest Group, not independently traded
                    r'\bCredit\s+Suisse\b', r'\bCS\b',
                    r'\bGoldman\s+Sachs\b', r'\bGS\b',
                    r'\bHalifax\b',  # Part of Lloyds Banking Group, not independently traded
                    r'\bHSBC\b', r'\bHSBC\b',
                    r'\bLloyds\b', r'\bLYG\b',
                    r'\bMetro\b', r'\bMTRO.L\b', r'\bMTRO\b', 
                    r'\bMorgan\s+Stanley\b', r'\bMS\b',
                    r'\bNatWest\b', r'\bNWG\b',
                    r'\bPNC\b', r'\bpnc\b', r'\bPNC\b',
                    r'\bSantander\b',  r'\bSAN\b', # traded in Spain as 'SAN'
                    r'\bSilicon\s+Valley\s+Bank\b', r'\bSVB\b', r'\bSIVBQ\b' 
                    r'\bStandard\s+Chartered\b', r'\bSCBFF\b',
                    r'\bTruist\b', r'\bTFC\b',
                    r'\bVirgin\s+Money\b', r'\bVMUK\b',
                    r'\bWells\s+Fargo\b', r'\bWFC\b',
                    r'\bRoyal\s+Bank\s+of\s+Scotland\b', r'\bRBS\b', r'\bNWG\b',  # Now rebranded as NatWest Group
                    r'\bThe\s+Co-operative\s+Bank\b',  # Not publicly traded 
                    r'\bTSB\s+Bank\b',  # TSB Banking Group plc was acquired by Banco Sabadell; not independently traded
                    r'\bYorkshire\s+Bank\b', r'\bCYBG\b',  # Part of Clydesdale and Yorkshire Banking Group, traded as 'CYBG' before its acquisition by Virgin Money
                    r'\bAllied\s+Irish\s+Bank\s+(UK)\b', r'\bAIBG.L\b'
                    ]
    
    subreddit = 'wallstreetbets'

    data_inst = GetData(reddit_client=reddit_client,
                        subreddit=subreddit,
                        search_patterns=ticker_patterns)

    df = data_inst.get_comments(last_run_time=last_run_time, comment_target=50)
    
    if len(df)==0:
        print("No new comments found since the last run.")
    
    else:
        # If new comments are found, process them
        print(f"Found {len(df)} new comments.")

        ### ADD RESULTS TO DB ####
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS comments (
            comment_id TEXT PRIMARY KEY,
            post_title TEXT,
            subreddit TEXT,
            comment_date TEXT,
            comment_author TEXT,
            comment TEXT,
            matched_phrase TEXT,
            upvotes INTEGER
        );
        """
        db.create_table(create_table_sql)
        db.insert_data_from_df(df=df, table_name='comments')

    # Update last run time
    db.update_last_run_time(datetime.now())

    
    # Close the database connection when done
    db.close_connection()

if __name__ == "__main__":
    main()