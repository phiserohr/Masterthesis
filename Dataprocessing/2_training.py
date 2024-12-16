#Labeling Tweets
import random
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from googletrans import Translator

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "**********"
DB_SCHEMA = "variance"

translator = Translator()

def connect_to_db():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    print("Database connection established.")
    return engine

#get a tweet
def get_random_tweet(engine):
    with engine.connect() as connection:
        query = f'SELECT id, tweet_text FROM {DB_SCHEMA}.filtered_tweets ORDER BY RANDOM() LIMIT 1'
        result = connection.execute(text(query)).fetchone()
        if result:
            tweet_id, tweet_text = result
            return tweet_id, tweet_text

#translate tweet
def translate_tweet(tweet_text):
    translation = translator.translate(tweet_text, dest='en')
    print("Original Tweet:", tweet_text)
    print("Translated Tweet:", translation.text)
    return translation.text

#Labeling
def label_and_store_tweet(engine, tweet_id, tweet_text, translated_text):
    try:
        label_input = input("1 for relevant, 0 for irrelevant, q to quit: ")
        if label_input.lower() == 'q':
            print("Quitting")
            return False
        
        label = int(label_input)
        
        with engine.connect() as connection:
            insert_query = text(
                f"""
                INSERT INTO {DB_SCHEMA}.manual_labels ("id", "tweet_text", "label")
                VALUES (:id, :text, :label)
                """
            )
            connection.execute(insert_query, {"id": tweet_id, "text": tweet_text, "label": label})
            connection.commit()
            print("Labeled.")
            return True

    except IntegrityError as e:
        print("Database error:", e)
    except ValueError as ve:
        print("Input error:", ve)
    except Exception as e:
        print("Unexpected error:", e)
    return False

def main():
    engine = connect_to_db()
    
    while True:
        tweet_id, tweet_text = get_random_tweet(engine)
        
        if not tweet_id:
            print("Done")
            break
        
        translated_text = translate_tweet(tweet_text)
        
        if translated_text:
            success = label_and_store_tweet(engine, tweet_id, tweet_text, translated_text)

    engine.dispose()
    print("Done")

if __name__ == "__main__":
    main()