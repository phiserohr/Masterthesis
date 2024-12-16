import sqlalchemy
from sqlalchemy import text  # Import text for SQL statements
from googletrans import Translator  # Requires 'googletrans==4.0.0-rc1' to be installed
import pandas as pd

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "NordProJect_123"
DB_SCHEMA = "variance"

# Connection string
db_connection_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Create the engine to connect to the database
engine = sqlalchemy.create_engine(db_connection_str)

# Initialize the translator
translator = Translator()

# Function to get the next unlabeled tweet
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

# Function to label a tweet
def label_tweet(tweet_id, label):
    query = text(f"""
    UPDATE {DB_SCHEMA}.sampled_tweets
    SET m_label = :label
    WHERE id = :tweet_id;
    """)
    with engine.connect() as conn:
        conn.execute(query, {"label": label, "tweet_id": tweet_id})
        conn.commit()  # Commit the update to the database

# Main loop for labeling tweets
while True:
    tweet = get_next_tweet()
    
    if tweet is None:
        print("All tweets have been labeled!")
        break

    tweet_id, tweet_text = tweet
    # Translate the tweet text to English
    translation = translator.translate(tweet_text, src='es', dest='en').text
    print("\nOriginal Tweet:", tweet_text)
    print("Translated Tweet:", translation)

    # Get the label from the user
    label = input("Enter label (1 for relevant, 0 for not relevant, or 'q' to quit): ")
    if label.lower() == 'q':
        print("Exiting labeling session.")
        break
    elif label in ['0', '1']:
        label_tweet(tweet_id, int(label))
        print(f"Tweet ID {tweet_id} labeled as {label}.")
    else:
        print("Invalid input, please enter 1, 0, or 'q' to quit.")
