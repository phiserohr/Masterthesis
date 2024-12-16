import psycopg2
from googletrans import Translator
import random

# Establishing connection to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="NordProJect_123"
        )
        cursor = conn.cursor()
        return conn, cursor
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None, None

# Create the manual_labels table to store labeled tweets
def create_labels_table(cursor):
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS variance.manual_labels (
                id SERIAL PRIMARY KEY,
                tweet_text TEXT,
                translated_text TEXT,
                label INTEGER
            );
        """)
        cursor.connection.commit()
        print("Table 'manual_labels' created successfully in schema 'variance'.")
    except Exception as e:
        print(f"Error creating table: {e}")

# Fetch random tweets from the filtered_tweets table
def fetch_random_tweets(cursor, sample_size=10):
    try:
        cursor.execute(f"""
            SELECT tweet_text FROM variance.filtered_tweets
            ORDER BY RANDOM()
            LIMIT {sample_size};
        """)
        rows = cursor.fetchall()
        return [row[0] for row in rows]  # List of tweet texts
    except Exception as e:
        print(f"Error fetching random tweets: {e}")
        return []

# Insert labeled data into the manual_labels table
def insert_label(cursor, tweet_text, translated_text, label):
    try:
        cursor.execute("""
            INSERT INTO variance.manual_labels (tweet_text, translated_text, label)
            VALUES (%s, %s, %s)
        """, (tweet_text, translated_text, label))
        cursor.connection.commit()
    except Exception as e:
        print(f"Error inserting label: {e}")

# Function for manual labeling
def manual_labeling(cursor, tweets):
    translator = Translator()

    for tweet_text in tweets:
        # Translate tweet
        translated_text = translator.translate(tweet_text, dest='en').text
        print(f"Original Tweet: {tweet_text}")
        print(f"Translated Tweet: {translated_text}")

        # Prompt for label
        while True:
            try:
                label = int(input("Enter 1 for relevant (literal landscape) or 0 for irrelevant (metaphorical/person): "))
                if label in [0, 1]:
                    break
                else:
                    print("Invalid input. Please enter 1 or 0.")
            except ValueError:
                print("Invalid input. Please enter 1 or 0.")

        # Save the labeled data to the database
        insert_label(cursor, tweet_text, translated_text, label)

# Main function to execute the manual labeling
def main():
    # Connect to the database
    conn, cursor = connect_to_db()
    if not conn:
        return

    # Create the labels table if it doesn't exist
    create_labels_table(cursor)

    # Fetch a sample of random tweets from the filtered_tweets table
    tweets = fetch_random_tweets(cursor, sample_size=100)  # Adjust sample_size as needed

    # Start manual labeling
    manual_labeling(cursor, tweets)

    # Close the connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
