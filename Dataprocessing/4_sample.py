#Labeling a Sample, Manually, to Check Machine Learning
#Step 4

import sqlalchemy
from sqlalchemy import text  
from googletrans import Translator  
import pandas as pd

# Database
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "************"
DB_SCHEMA = "variance"

# Connection string
db_connection_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = sqlalchemy.create_engine(db_connection_str)

translator = Translator()

# Function To Get unlabeld Tweet
def get_next_tweet():
    query = text(f"""
    SELECT id, tweet_text
    FROM {DB_SCHEMA}.sampled_tweets
    WHERE m_label IS NULL
    LIMIT 1;
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        tweet = result.fetchone()
    return tweet

# Function for Labeling
def label_tweet(tweet_id, label):
    query = text(f"""
    UPDATE {DB_SCHEMA}.sampled_tweets
    SET m_label = :label
    WHERE id = :tweet_id;
    """)
    with engine.connect() as conn:
        conn.execute(query, {"label": label, "tweet_id": tweet_id})
        conn.commit() 

# Main Loop
while True:
    tweet = get_next_tweet()
    
    if tweet is None:
        print("Done. :D")
        break

    tweet_id, tweet_text = tweet
    translation = translator.translate(tweet_text, src='es', dest='en').text
    print("\nOriginal Tweet:", tweet_text)
    print("Translated Tweet:", translation)

    label = input("Enter label (1 for relevant, 0 for not relevant, or 'q' to quit): ")
    if label.lower() == 'q':
        print("Exiting labeling session.")
        break
    elif label in ['0', '1']:
        label_tweet(tweet_id, int(label))
        print(f"Tweet ID {tweet_id} labeled as {label}.")
    else:
        print("Enter 1, 0 or q")
