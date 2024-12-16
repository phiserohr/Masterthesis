import pandas as pd
from sqlalchemy import create_engine

# Create a connection to the PostgreSQL database
def connect_to_db():
    try:
        engine = create_engine("postgresql+psycopg2://postgres:NordProJect_123@localhost/postgres")
        return engine
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

# Function to save tweets for a specific term and day
def save_tweets_for_term_and_day(engine, term, day_of_year, output_file):
    query = f"""
    SELECT found_term AS term, created_at, tweet_text
    FROM variance.labeled_tweets
    WHERE "Label" = 1
      AND found_term = '{term}'
      AND EXTRACT(DOY FROM created_at) = {day_of_year};
    """
    tweets = pd.read_sql(query, engine)
    if tweets.empty:
        print(f"No tweets found for term '{term}' on day {day_of_year}.")
    else:
        tweets.to_csv(output_file, index=False)
        print(f"Saved {len(tweets)} tweets for term '{term}' on day {day_of_year} to {output_file}.")

def main():
    # Connect to the database
    engine = connect_to_db()
    if engine is None:
        exit()

    # Specify term and day of the year
    hotspot_term = "cascada"  # Replace with the desired term
    hotspot_day = 146       # Replace with the desired day of year
    output_file = f"hotspot_tweets_{hotspot_term}_day_{hotspot_day}.csv"

    # Save tweets for the specified term and day
    save_tweets_for_term_and_day(engine, hotspot_term, hotspot_day, output_file)

    # Close the engine connection
    engine.dispose()

if __name__ == "__main__":
    main()


