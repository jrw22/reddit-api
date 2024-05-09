import pandas as pd
import datetime
import logging
import re

class GetData:
    def __init__(self, reddit_client, firm_list_path: str, subreddits: list):
        self.reddit_client = reddit_client
        self.firm_list_path = firm_list_path
        self.subreddits = subreddits
    
    def load_ticker_patterns_from_csv(self):
        df = pd.read_csv(self.firm_list_path)
        # Combine the 'name', 'abbreviation', and 'ticker' into a single list, excluding any NaN values
        patterns = df[['name','altname', 'abbreviation', 'ticker','altticker']].fillna('').apply(lambda x: '|'.join(x[x != '']), axis=1).tolist()
        # Ensure patterns are unique and non-empty
        patterns = [pattern for pattern in set(patterns) if pattern]
        return patterns

    def add_word_boundaries(self, search_patterns):
        updated_patterns = []
        for pattern in search_patterns:
            terms = pattern.split('|')  # Split pattern into terms
            bounded_terms = [r'\b' + term + r'\b' for term in terms]  # Add boundaries to each term
            updated_patterns.append('|'.join(bounded_terms))  # Rejoin the terms
        return updated_patterns
    
    @staticmethod
    def clean_comments(df):
        """
        Clean columns in df, and where no comments are found, create 
        the columns with appropriate default values.
        """
        # Handle 'matched_phrase' specifically if it exists
        if 'matched_phrase' in df.columns:
            df['matched_phrase'] = df['matched_phrase'].str.replace(r'\\b|\\s|\+', '', regex=True)

        # List of all expected columns with their default values
        expected_columns_with_defaults = {
            'subreddit': '', 
            'post_title': '', 
            'comment_id': '', 
            'comment_date': '',  
            'comment': '', 
            'matched_phrase': '',  # Ensuring it's listed even though it's handled above
            'upvotes': 0,  # Assuming 'upvotes' is numeric
        }

        # Add missing columns with their default values
        for column, default_value in expected_columns_with_defaults.items():
            if column not in df.columns:
                df[column] = default_value

        return df


    def get_comments(self, comment_target, last_run_time):
        data = []
        seen_comment_ids = set()
        comment_counter = 0
        one_day_ago = datetime.datetime.now() - datetime.timedelta(days=1)

        # Load search patterns
        firms = self.load_ticker_patterns_from_csv()
        firms_with_boundaries = self.add_word_boundaries(firms)
        # Compile the patterns into a regular expression
        pattern_re = re.compile('|'.join(firms_with_boundaries), re.IGNORECASE)

        for subreddit in self.subreddits:
            logging.info(f"Fetching from r/{subreddit}...")
            subreddit_data = self.reddit_client.subreddit(subreddit)

            for post in subreddit_data.new(limit=None):  # Fetch as many as needed, but filter by date
                post_creation_time = datetime.datetime.fromtimestamp(post.created_utc)
                if post_creation_time < one_day_ago:
                    continue

                logging.info("Processing post: {}".format(post.title))
                post.comments.replace_more(limit=0)
                for comment in post.comments.list():
                    if comment.id in seen_comment_ids or datetime.datetime.fromtimestamp(comment.created_utc) <= last_run_time:
                        continue

                    if pattern_re.search(comment.body):
                        comment_date = datetime.datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
                        data.append({
                            'subreddit': subreddit,
                            'post_title': post.title,
                            'comment_id': comment.id,
                            'comment_date': comment_date,
                            'comment': comment.body,
                            'matched_phrase': pattern_re.search(comment.body).group(0), 
                            'upvotes': comment.score
                        })
                        seen_comment_ids.add(comment.id)

                    comment_counter += 1
                    if len(data) >= comment_target:
                        break

                if len(data) >= comment_target:
                    logging.info("Reached comment target.")
                    break

        logging.info(f"Total comments checked: {comment_counter}")
        logging.info(f"Total comments collected: {len(data)}")

        # Convert the list of dictionaries into a pandas DataFrame
        return pd.DataFrame(data)