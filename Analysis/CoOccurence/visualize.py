#script to plot wordclouds of co occurence
#change term at the bottom

import pandas as pd
from sqlalchemy import create_engine
from collections import Counter
import spacy
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Database connection details
db_connection_str = "postgresql://postgres:************@localhost/postgres"
engine = create_engine(db_connection_str)

def fetch_tweets_with_term(engine, target_term):
    """Fetch tweets containing the target term."""
    query = f"""
    SELECT tweet_text
    FROM variance.labeled_tweets
    WHERE LOWER(found_term) = '{target_term}'
      AND "Label" = 1;
    """
    with engine.connect() as conn:
        tweets = pd.read_sql(query, conn)
    if tweets.empty:
        raise ValueError(f"No relevant tweets found for '{target_term}'.")
    return tweets['tweet_text'].tolist()

def count_co_occurrences(tweets, target_term, window_size=3):
    """Tokenize and count co-occurring words."""
    nlp = spacy.load("es_core_news_sm")
    co_occurrence_counter = Counter()

    for tweet in tweets:
        doc = nlp(tweet.lower())
        words = [token.text for token in doc if token.is_alpha]
        indices = [i for i, word in enumerate(words) if word == target_term]

        for index in indices:
            start = max(index - window_size, 0)
            end = min(index + window_size + 1, len(words))
            context = words[start:end]
            for word in context:
                if word != target_term:
                    co_occurrence_counter[word] += 1

    return co_occurrence_counter

def generate_wordcloud(co_occurrences, target_term):
    """Generate and display a word cloud."""
    wordcloud = WordCloud(
        width=800,
        height=400,
        background_color="white",
        colormap="viridis"
    ).generate_from_frequencies(co_occurrences)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(f"Word Cloud of '{target_term}' Co-occurrences", fontsize=14)
    plt.show()

def main():
    target_term = "playa"
    window_size = 3

    try:
        print(f"Fetching relevant tweets containing '{target_term}'...")
        tweets = fetch_tweets_with_term(engine, target_term)

        print(f"Counting co-occurrences around '{target_term}'...")
        co_occurrences = count_co_occurrences(tweets, target_term, window_size)

        print(f"Generating word cloud for '{target_term}'...")
        generate_wordcloud(co_occurrences, target_term)

    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()
