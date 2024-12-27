import pandas as pd

def concat_text(posts: pd.DataFrame):
    posts["fulltext"] = posts["title"] + posts["selftext"]
    return posts

    
