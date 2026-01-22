import os
import logging
import tweepy
from .. import config

class XClient:
    def __init__(self):
        self.api_key = config.X_API_KEY
        self.api_secret = config.X_API_SECRET
        self.access_token = config.X_ACCESS_TOKEN
        self.access_token_secret = config.X_ACCESS_TOKEN_SECRET
        
        if not all([self.api_key, self.api_secret, self.access_token, self.access_token_secret]):
            logging.warning("X API credentials not fully configured. Social media posting will be disabled.")
            self.client = None
        else:
            try:
                self.client = tweepy.Client(
                    consumer_key=self.api_key,
                    consumer_secret=self.api_secret,
                    access_token=self.access_token,
                    access_token_secret=self.access_token_secret
                )
                logging.info("XClient initialized successfully.")
            except Exception as e:
                logging.error(f"Failed to initialize XClient: {e}")
                self.client = None

    def post_tweet(self, text: str):
        """Posts a tweet to X."""
        if not self.client:
            logging.error("XClient is not initialized. Cannot post tweet.")
            return None

        try:
            response = self.client.create_tweet(text=text)
            logging.info(f"Tweet posted successfully. ID: {response.data['id']}")
            return response.data['id']
        except tweepy.TweepyException as e:
            logging.error(f"Failed to post tweet: {e}")
            return None
