import random
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from googletrans import Translator

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "NordProJect_123"
DB_SCHEMA = "variance"

# Initialize Google Translator
translator = Translator()

def connect_to_db():
    """Establishes a connection to the PostgreSQL database using SQLAlchemy."""
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
        print("Database connection established.")
        return engine
    except Exception as e:
        print("Error connecting to the database:", e)
        raise

def get_random_tweet(engine):
    """Fetches one random tweet from the filtered_tweets table."""
    with engine.connect() as connection:
        query = f'SELECT id, tweet_text FROM {DB_SCHEMA}.filtered_tweets ORDER BY RANDOM() LIMIT 1'
        result = connection.execute(text(query)).fetchone()
        if result:
            tweet_id, tweet_text = result
            return tweet_id, tweet_text
        else:
            print("No tweets found in the filtered_tweets table.")
            return None, None

def translate_tweet(tweet_text):
    """Translates the tweet text into English."""
    try:
        translation = translator.translate(tweet_text, dest='en')
        print("Original Tweet:", tweet_text)
        print("Translated Tweet:", translation.text)
        return translation.text
    except Exception as e:
        print("Error translating tweet:", e)
        return None

def label_and_store_tweet(engine, tweet_id, tweet_text, translated_text):
    """Prompts user to label the tweet as relevant (1) or not relevant (0) and stores it in manual_labels."""
    try:
        label = input("Enter 1 if the tweet is relevant, or 0 if not relevant: ")
        label = int(label)
        if label not in [0, 1]:
            print("Invalid label. Please enter either 1 or 0.")
            return False
        
        # Insert labeled tweet into manual_labels
        with engine.connect() as connection:
            insert_query = text(
                f"""
                INSERT INTO {DB_SCHEMA}.manual_labels ("ID", "Tweet Text", "Label")
                VALUES (:id, :text, :label)
                """
            )
            result = connection.execute(insert_query, {"id": tweet_id, "text": tweet_text, "label": label})
            connection.commit()  # Ensure changes are committed
            print("Tweet labeled and stored successfully.")
            return True
    except IntegrityError:
        print("This tweet has already been labeled.")
        return False
    except ValueError:
        print("Please enter a valid integer for the label.")
        return False
    except Exception as e:
        print("Error labeling and storing tweet:", e)
        return False

def main():
    engine = connect_to_db()
    
    while True:
        tweet_id, tweet_text = get_random_tweet(engine)
        
        if not tweet_id:
            print("No more unlabeled tweets available.")
            break
        
        translated_text = translate_tweet(tweet_text)
        
        if translated_text:
            success = label_and_store_tweet(engine, tweet_id, tweet_text, translated_text)
            if not success:
                print("Retrying...")

    engine.dispose()
    print("Finished labeling all available tweets.")

if __name__ == "__main__":
    main()
