import praw
import truststore
import time

class APIQueryCounter:
    def __init__(self,):
        self.query_count = 0
        self.start_time = time.time()

    def increment(self):
        self.query_count =+ 1
    
    def get_count(self):
        return self.query_count
    
    def reset(self):
        self.query_count = 0
        self.start_time = time.time()
        
class APIConnection:
    def __init__(self, client_id, client_secret, user_agent):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.api_counter = APIQueryCounter()

    def initialise_client(self):
        truststore.inject_into_ssl()
        reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )
        return reddit
    
    def make_api_call(self, function, *args, **kwargs):
        self.api_counter.increment()
        return function(*args, **kwargs)
    
    def get_total_calls(self):
        return self.api_counter.get_count()

