import pandas as pd
import datetime
import logging
import re

class GetData:
    def __init__(self, reddit_client, subreddit: str, search_patterns: list):
        self.reddit_client = reddit_client
        self.subreddit = subreddit
        self.search_patterns = search_patterns

    def get_comments(self, comment_target: int, last_run_time: datetime.datetime):
        data = []
        seen_comment_ids = set()
        logging.info(f"Fetching from r/{self.subreddit}...")
        subreddit_data = self.reddit_client.subreddit(self.subreddit)

        comment_counter = 0
        one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)

        for post in subreddit_data.new(limit=None):  # Fetch as many as needed, but filter by date
            post_creation_time = datetime.datetime.fromtimestamp(post.created_utc)

            # Skip posts older than one day - else it will search through all posts looking for new comments
            if post_creation_time < one_day_ago:
                continue

            logging.info("Processing post: {}".format(post.title))
            post.comments.replace_more(limit=0)
            for comment in post.comments.list():
                if comment.id in seen_comment_ids or datetime.datetime.fromtimestamp(comment.created_utc) <= last_run_time:
                    continue

                comment_counter += 1
                matched_pattern = next((pattern for pattern in self.search_patterns if re.search(pattern, comment.body, re.IGNORECASE)), None)
                if matched_pattern:
                    comment_date = datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                    data.append({
                        'subreddit': self.subreddit,
                        'post_title': post.title,
                        'comment_id': comment.id,
                        'comment_date': comment_date,
                        'comment_author': str(comment.author),
                        'comment': comment.body,
                        'matched_phrase': matched_pattern, 
                        'upvotes': comment.score
                    })
                    seen_comment_ids.add(comment.id)

                if len(data) >= comment_target:
                    break

            if len(data) >= comment_target:
                logging.info("Reached comment target.")
                break

        logging.info(f"Total comments checked: {comment_counter}")
        logging.info(f"Total comments collected: {len(data)}")

        # Convert the list of dictionaries into a pandas DataFrame
        df = pd.DataFrame(data)

        return df

    def clean_comments(df):
        df['matched_phrase'] = df['matched_phrase'].str.replace(r'\\b', '', regex=True)
        df['matched_phrase'] = df['matched_phrase'].str.replace(r'\\s', ' ', regex=True)
        df['matched_phrase'] = df['matched_phrase'].str.replace(r'\+', '', regex=True)

        return df
    
    ## Function to try and get credit suisse and SVB posts from a year ago
    # def get_posts(self, post_target: int):
    #     data = []
    #     logging.info(f"Fetching from r/{self.subreddit}...")
    #     subreddit_data = self.reddit_client.subreddit(self.subreddit)

    #     post_counter = 0
    #     current_year = datetime.datetime.utcnow().year
    #     start_date = datetime.datetime(current_year - 1, 2, 1)  # Corrected to February 1st
    #     end_date = datetime.datetime(current_year - 1, 8, 31)  # August 31st

    #     for post in subreddit_data.new(limit=None):
    #         post_creation_time = datetime.datetime.fromtimestamp(post.created_utc)

    #         if post_creation_time < start_date or post_creation_time > end_date:
    #             continue

    #         logging.info(f"Processing post: {post.title}")
    #         post_counter += 1
    #         matched_pattern = next((pattern for pattern in self.search_patterns if re.search(pattern, post.title, re.IGNORECASE)), None)
    #         if matched_pattern:
    #             post_date = post_creation_time.strftime('%Y-%m-%d %H:%M:%S')
    #             data.append({
    #                 'subreddit': self.subreddit,
    #                 'post_title': post.title,
    #                 'matched_phrase': matched_pattern, 
    #                 'upvotes': post.score
    #             })

    #         if len(data) >= post_target:
    #             logging.info("Reached post target.")
    #             break

    #     logging.info(f"Total posts checked: {post_counter}")
    #     logging.info(f"Total posts collected: {len(data)}")

    #     df = pd.DataFrame(data)
    #     return df