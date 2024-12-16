# File for Filtering the Tweets
import os
import pandas as pd
import psycopg2
import re

# Databaseconnection
def connect_to_db():
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="*************"
        )
        cursor = conn.cursor()
        return conn, cursor
    
# Create Table
def create_filtered_tweets_table(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS variance.filtered_tweets;
       """)
    
    cursor.execute("""
        CREATE TABLE variance.filtered_tweets (
            id SERIAL PRIMARY KEY,
            tweet_text TEXT NOT NULL,
            found_term TEXT,
            created_at TIMESTAMP,  
            user_location TEXT,    
            place_full_name TEXT, 
            geo_latitude DOUBLE PRECISION,  
            geo_longitude DOUBLE PRECISION  
        );
    """)
    cursor.connection.commit()

# Get Terms
def fetch_landscape_terms(cursor):
    try:
        cursor.execute("SELECT main_term_sp, synonyms FROM variance.landscape_terms WHERE relevant = TRUE;")
        rows = cursor.fetchall()
        terms = []
        for row in rows:
            main_term = row[0].lower()
            terms.append(main_term)
            synonyms = row[1].split(',') if row[1] else []
            terms.extend([synonym.strip().lower() for synonym in synonyms])
        return terms
    except Exception as e:
        print(f"Error fetching landscape terms: {e}")
        return []

# Tokenizing
def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())

# Insert Data
def insert_single_entry(cursor, entry):
    try:
        insert_query = """
            INSERT INTO variance.filtered_tweets (tweet_text, found_term, created_at, user_location, place_full_name, geo_latitude, geo_longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, entry)
        cursor.connection.commit()
    except Exception as e:
        print(f"Error inserting entry: {e}")

# Process CSV's
def process_csv_file(file_path, landscape_terms, cursor):
    try:
        df = pd.read_csv(file_path, sep='\t', on_bad_lines='skip')

        required_columns = ['text', 'created_at', 'user_location', 'place_full_name', 'geo_latitude', 'geo_longitude']
        if not all(col in df.columns for col in required_columns):
            print(f"Skipping file {file_path} because it doesn't contain all required columns.")
            return 0, 0, 0

        total_tweets_before_filtering = len(df)

        tweets_processed, terms_saved = 0, 0
        for index, row in df.iterrows():
            tweet_text = str(row['text']).lower()
            if tweet_text.startswith("rt "):  # Skip retweets
                continue

            created_at = row['created_at'] if not pd.isna(row['created_at']) else None
            user_location = str(row['user_location']) if not pd.isna(row['user_location']) else 'unknown'
            place_full_name = str(row['place_full_name']) if not pd.isna(row['place_full_name']) else 'unknown'
            geo_latitude = row['geo_latitude'] if not pd.isna(row['geo_latitude']) else None
            geo_longitude = row['geo_longitude'] if not pd.isna(row['geo_longitude']) else None

            tokens = tokenize(tweet_text)

            for term in landscape_terms:
                if term in tokens:
                    tweets_processed += 1
                    entry = (tweet_text, term, created_at, user_location, place_full_name, geo_latitude, geo_longitude)
                    insert_single_entry(cursor, entry)
                    terms_saved += 1

        return total_tweets_before_filtering, tweets_processed, terms_saved
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return 0, 0, 0

# Process all CSV files in the specified directory
def process_all_csv_files(csv_directory, landscape_terms, cursor):
    total_tweets_before_filtering = 0
    total_tweets_processed = 0
    total_terms_saved = 0

    for root, dirs, files in os.walk(csv_directory):
        for filename in files:
            if filename.endswith('.csv'):
                file_path = os.path.join(root, filename)
                print(f"Processing file: {file_path}")
                tweets_before_filtering, tweets_processed, terms_saved = process_csv_file(file_path, landscape_terms, cursor)
                total_tweets_before_filtering += tweets_before_filtering
                total_tweets_processed += tweets_processed
                total_terms_saved += terms_saved

                print(f"File {file_path}: Total tweets before filtering: {tweets_before_filtering}, Tweets processed: {tweets_processed}, Terms saved: {terms_saved}")

    print(f"Finished processing all files in {csv_directory}. Total tweets before filtering: {total_tweets_before_filtering}, Total tweets processed: {total_tweets_processed}, Total terms saved: {total_terms_saved}")

# Main
def main():
    conn, cursor = connect_to_db()
    if not conn:
        return

    create_filtered_tweets_table(cursor)

    landscape_terms = fetch_landscape_terms(cursor)
    if not landscape_terms:
        print("No landscape terms found.")
        return

    # Process all CSV files in the folder '201'
    process_all_csv_files('/Users/lippi/Library/Mobile Documents/com~apple~CloudDocs/04_Master/MasterThesis/Python/2017', landscape_terms, cursor)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
