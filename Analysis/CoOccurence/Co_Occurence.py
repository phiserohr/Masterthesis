import pandas as pd
from collections import Counter
from sqlalchemy import create_engine
from math import log
import spacy
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Load Spanish language model
nlp = spacy.load("es_core_news_sm")

# Database connection details
db_connection_str = "postgresql://postgres:NordProJect_123@localhost/postgres"
engine = create_engine(db_connection_str)

# List of target terms
target_terms = [
    "tierra", "mar", "playa", "río", "costa", "naturaleza", "cerro", "montaña",
    "bosque", "lago", "desierto", "paisaje", "volcán", "colina", "amanecer",
    "madrugada", "atardecer", "anochecer", "costa", "monte", "cumbre", "cabezo"
]

# Fetch relevant tweets containing the target term
def fetch_tweets_with_term(engine, target_term):
    query = f"""
    SELECT tweet_text
    FROM variance.labeled_tweets
    WHERE LOWER(found_term) = '{target_term}'
      AND "Label" = 1;
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Calculate log likelihood and top terms
def calculate_log_likelihood(tweets, target_term, window_size=3):
    co_occurrence_counter = Counter()
    total_word_count = Counter()

    for tweet in tweets['tweet_text']:
        # Tokenize and filter out unwanted tokens
        doc = nlp(tweet.lower())
        words = [
            token.text for token in doc
            if token.is_alpha and token.pos_ in {"NOUN", "VERB", "ADJ"} \
            and not token.text.startswith(("http", "@"))
        ]

        indices = [i for i, word in enumerate(words) if word == target_term]

        for index in indices:
            start = max(index - window_size, 0)
            end = min(index + window_size + 1, len(words))
            context = words[start:end]
            for word in context:
                if word != target_term:
                    co_occurrence_counter[word] += 1
            total_word_count.update(context)

    total = sum(total_word_count.values())
    results = []

    for word, observed in co_occurrence_counter.items():
        if observed < 2:
            continue

        row_sum = observed
        col_sum = total_word_count[word]

        R2 = total - row_sum
        C2 = total - col_sum

        E11 = (row_sum * col_sum) / total
        E12 = (row_sum * C2) / total
        E21 = (R2 * col_sum) / total
        E22 = (R2 * C2) / total

        O11 = observed
        O12 = row_sum - observed
        O21 = col_sum - observed
        O22 = total - (O11 + O12 + O21)

        LL = 0
        for O, E in zip([O11, O12, O21, O22], [E11, E12, E21, E22]):
            if O > 0: 
                LL += O * log(O / E)
        LL *= 2

        # Log: for easy numbers
        log_LL = log(LL) if LL > 0 else 0

        results.append({"Word": word, "Log_LL": log_LL, "Freq_co_occurrence": observed})

    return pd.DataFrame(results)

# Generate and save a word cloud
def generate_word_cloud(results_df, target_term):
    word_dict = dict(zip(results_df["Word"], results_df["Log_LL"]))
    wordcloud = WordCloud(width=800, height=400, background_color="white", colormap="viridis")
    wordcloud.generate_from_frequencies(word_dict)

    # Save word cloud
    wordcloud.to_file(f"{target_term}_wordcloud.png")
    print(f"Word cloud saved as {target_term}_wordcloud.png")

# Main function
def main():
    for target_term in target_terms:
        print(f"Processing '{target_term}'...")
        tweets = fetch_tweets_with_term(engine, target_term)

        if tweets.empty:
            print(f"No relevant tweets found containing '{target_term}'.")
            continue

        print(f"Calculating Log Likelihood values for '{target_term}'...")
        results_df = calculate_log_likelihood(tweets, target_term)

        top_results = results_df.sort_values(by="Log_LL", ascending=False).head(25)
        print(top_results)

        # Save top results to CSV
        top_results.to_csv(f"{target_term}_top_terms.csv", index=False)
        print(f"Top terms saved to {target_term}_top_terms.csv")

        # Generate and save word cloud
        generate_word_cloud(results_df, target_term)

# Run the script
if __name__ == "__main__":
    main()
