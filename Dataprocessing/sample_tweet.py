from sqlalchemy import create_engine, text

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "NordProJect_123"
DB_SCHEMA = "variance"

# Connection string
db_connection_str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# Create the engine to connect to the database
engine = create_engine(db_connection_str)

# Step 1: Create the table structure explicitly and commit immediately after
create_sampled_table_query = text(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.sampled_tweets (
    id int8,
    tweet_text text,
    found_term text,
    created_at timestamp,
    user_location text,
    place_full_name text,
    geo_latitude float8,
    geo_longitude float8,
    "Label" int8,
    m_label int
);
""")

# Execute the table creation and commit
with engine.connect() as conn:
    conn.execute(create_sampled_table_query)
    conn.commit()  # Explicitly commit table creation
    print("Table 'sampled_tweets' created or verified to exist.")

# Step 2: Insert 700 random samples with the new table structure
sample_query = text(f"""
INSERT INTO {DB_SCHEMA}.sampled_tweets (id, tweet_text, found_term, created_at, user_location, place_full_name, geo_latitude, geo_longitude, "Label", m_label)
SELECT id, tweet_text, found_term, created_at, user_location, place_full_name, geo_latitude, geo_longitude, "Label", NULL AS m_label
FROM {DB_SCHEMA}.labeled_tweets
ORDER BY RANDOM()
LIMIT 700;
""")

# Execute the sampling query
with engine.connect() as conn:
    conn.execute(sample_query)
    conn.commit()  # Commit after insertion
    print("Sample of 700 tweets inserted into 'sampled_tweets' with 'm_label' column.")
