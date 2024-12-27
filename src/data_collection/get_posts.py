from data_collection.collector import DataCollector
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

def get_posts():
    collector = DataCollector(**credentials)
    users = pd.read_csv("./data1/user_karma.csv").iloc[1999:]
    collector.get_users_posts(users)

def main():
    get_posts()