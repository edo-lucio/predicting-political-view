import pandas as pd
import json
import os
import argparse 
from utils import read_json
import logging

from data_collection.collector import DataCollector
from data_collection.processing import concat_text

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load credentials and configuration
credentials_json = os.getenv('REDDIT_API_CREDENTIALS')
credentials = json.loads(credentials_json)[0]

config = read_json("./config/collection_config.json")["reddit"]
COLLECTION_CONFIGS = config["collection_configs"]

# Function to get the last user index from a file
def get_last_collected_user(posts_path):
    if os.path.exists(posts_path):
        posts = pd.read_csv(posts_path)
        last_collected_user = posts.iloc[-1]["username"]
        return last_collected_user
    else:
        return 0
    
def get_user_index(users, username):
    if not username:
        return 0
    return users[users["username"] == username].index[0]

def collect_data(collection_configs: dict, content: str = "skip", users_limit: int = 1000, posts_limit: int = float("inf"), member_count: bool = False):
    collector = DataCollector(**credentials)

    if content == "posts":
        users = pd.read_csv("./data/users.csv")
        last_user = get_last_collected_user("./data/posts.csv")
        last_user_index = get_user_index(users, last_user)
        users = users.iloc[last_user_index:]

        collector.get_users_posts(users["username"].tolist(), limit=posts_limit)

    elif content == "all":
        users = collector.get_users(collection_configs["subreddit"], limit=users_limit)
        collector.get_users_posts(users["username"].tolist())

    posts = pd.read_csv("./data/posts.csv").drop_duplicates()
    posts = concat_text(posts)

    posts.to_csv("./data/posts.csv", index=False)

    if member_count: 
        political_subreddits = pd.read_csv("./data/political_subreddits.csv")["subreddit"].tolist()
        posts = pd.read_csv("./data/posts.csv")
        posts["member_count"] = posts[posts["subreddit"].isin(political_subreddits)]["subreddit"].apply(lambda x: collector.get_subreddit_member_count(x))
        
        posts.to_csv("./data/posts.csv", index=False)

def main():
    parser = argparse.ArgumentParser(description="Reddit Data Collection Script")
    parser.add_argument("--content", type=str, choices=["all", "posts", "skip"], default="posts",
                        help="Specify the type of content to collect: 'all' or 'posts'. Default is 'skip'.")
    
    parser.add_argument("--users", type=int, default="1000",
                        help="Specify the number of users to collect: int.")
    
    parser.add_argument("--posts", type=float, default=float("inf"),
                        help="Specify the number of submissions to collect: int.")
    
    parser.add_argument("--members", type=bool, default="False",
                    help="Specify the number of submissions to collect: int.")
    
    args = parser.parse_args()

    collect_data(COLLECTION_CONFIGS, content=args.content)

if __name__ == "__main__":
    main()
