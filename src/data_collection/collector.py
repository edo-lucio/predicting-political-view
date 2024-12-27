import praw
from datetime import datetime
import pandas as pd
import json
import os
from utils import read_json
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

credentials_json = os.getenv('REDDIT_API_CREDENTIALS')
credentials = json.loads(credentials_json)[0]

config = read_json("./config/collection_config.json")["reddit"]
COLLECTION_CONFIGS = config["collection_configs"]
SUBMISSIONS_TYPES = ['top', 'controversial', 'new', 'hot', 'rising']

def _convert_columns_to_lowercase(df, columns):
    """
    Convert specified columns to lowercase in both DataFrames.

    Parameters:
    df1 (pd.DataFrame): The first DataFrame.
    df2 (pd.DataFrame): The second DataFrame.
    columns (list): List of column names to convert to lowercase.

    Returns:
    pd.DataFrame, pd.DataFrame: The modified DataFrames with specified columns in lowercase.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].str.lower()
            df[col] = df[col].astype(str)

    return df

class DataCollector:
    def __init__(
            self, 
            client_id,
            client_secret,
            username,
            password,
            user_agent="User-Agent: Mozilla/5.0 (<system-information>) <platform> (<platform-details>) <extensions>"
            ):

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            username=username,
            password=password
        )

    def get_users(self, subreddit_name, limit=1000, output_file="./data1/users.csv"):
        if not os.path.exists(output_file):
            pd.DataFrame(columns=["username", "subreddit"]).to_csv(output_file, index=False, encoding='utf-8')

        praw_subreddit = self.reddit.subreddit(subreddit_name)
        all_users_data = []  # This stores all user data for returning the final result
        users = set()  # Set to track unique users

        for submission_type in SUBMISSIONS_TYPES:
            users_data = []  # Temporarily holds data for each submission batch
            for submission in getattr(praw_subreddit, submission_type)(limit=1000):
                if len(users) >= limit:
                    break
                if not submission.author or not submission.author.name:
                    continue

                username = submission.author.name

                if username not in users and str(username).lower() != "automoderator":
                    users.add(username)
                    users_data.append({"username": username, "subreddit": subreddit_name})
                    all_users_data.append({"username": username, "subreddit": subreddit_name})
                    logger.info(f"Added user {username} in subreddit {subreddit_name}")

                submission.comments.replace_more(limit=0)  # To fetch all comments
                for comment in submission.comments.list():
                    if len(users) >= limit:
                        break
                    if not comment.author or not comment.author.name:
                        continue
                    username = comment.author.name
                    if username not in users and str(username).lower() != "automoderator":
                        users.add(username)
                        users_data.append({"username": username, "subreddit": subreddit_name})
                        all_users_data.append({"username": username, "subreddit": subreddit_name})
                        logger.info(f"Added user {username} in subreddit {subreddit_name}")

            if users_data:
                users_df = pd.DataFrame(users_data)
                users_df = _convert_columns_to_lowercase(users_df, ["subreddit", "username"])
                users_df.to_csv(output_file, mode='a', index=False, header=not pd.io.common.file_exists(output_file), encoding='utf-8')

        return pd.DataFrame(all_users_data)

    def get_users_posts(self, usernames, limit=float("inf"), output_file="./data1/posts.csv"):
        def get_content(submission, submission_type: int):
            posted_time = datetime.fromtimestamp(submission.created_utc).strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(submission, praw.models.reddit.comment.Comment):
                title = ""
                selftext = submission.body.replace("\n", "")
            else:
                title = submission.title.replace("\n", "") if submission.title else ""
                selftext = submission.selftext.replace("\n", "")
            post = {
                "username": username,
                "title": title,
                "selftext": selftext,
                "subreddit": str(submission.subreddit),
                "score": submission.score,
                "num_comments": submission.num_comments,
                "posted_time": posted_time, 
                "submission_type": submission_type
                }   
            
            return post
        
        if not os.path.exists(output_file):
            pd.DataFrame(columns=["username", "title", "selftext", "subreddit", "score", "num_comments", "posted_time", "submission_type"]).to_csv(output_file, index=False, encoding='utf-8')
        all_posts = []

        for username in usernames:
            posts_data = []
            praw_user = self.reddit.redditor(username)
            try: 
                for comment in praw_user.comments.new(limit=1000):
                    if len(posts_data) >= limit: 
                        break

                    content = get_content(comment, 1)
                    posts_data.append(content)
                    all_posts.append(content)

                for comment in praw_user.comments.controversial(limit=1000, time_filter='year'):
                    if len(posts_data) >= limit: 
                        break

                    content = get_content(comment, 1)
                    posts_data.append(content)
                    all_posts.append(content)

                for submission in praw_user.submissions.new(limit=1000):
                    if len(posts_data) >= limit: 
                        break
                    content = get_content(submission, 0)
                    posts_data.append(content)
                    all_posts.append(content)
            except Exception as e:
                logger.info(f"Couldn't get {username}'s post: {e}")

            posts_df = pd.DataFrame(posts_data)
            posts_df = _convert_columns_to_lowercase(posts_df, ["subreddit", "username"])
            posts_df.to_csv(output_file, index=False, mode='a', header=not pd.io.common.file_exists(output_file))
            logger.info(f"Added {len(posts_data)} posts for {username}")

        return pd.DataFrame(all_posts)

    def get_subreddit_member_count(self, subreddit_name):
        """
        Retrieve the number of members (subscribers) in a subreddit.
        
        Args:
        subreddit_name (str): The name of the subreddit.
        
        Returns:
        int: The number of members (subscribers) of the subreddit.
        """
        subreddit = self.reddit.subreddit(subreddit_name)
        return subreddit.subscribers

