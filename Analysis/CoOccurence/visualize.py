import pandas as pd
import networkx as nx
from sqlalchemy import create_engine
from wordcloud import WordCloud
import plotly.graph_objects as go
import matplotlib.pyplot as plt

# Database connection details
db_connection_str = "postgresql://postgres:NordProJect_123@localhost/postgres"
engine = create_engine(db_connection_str)

# Fetch relevant tweets containing the landscape term
def fetch_tweets_with_term(engine, target_term):
    query = f"""
    SELECT tweet_text
    FROM variance.labeled_tweets
    WHERE LOWER(found_term) = '{target_term}'
      AND "Label" = 1;
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Tokenize and count co-occurring words
def count_co_occurrences(tweets, target_term, window_size=3):
    from collections import Counter
    import spacy
    nlp = spacy.load("es_core_news_sm")
    co_occurrence_counter = Counter()
    total_word_count = Counter()

    for tweet in tweets['tweet_text']:
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
            total_word_count.update(words)

    return co_occurrence_counter, total_word_count

# Generate and display a network graph
def generate_network_graph(co_occurrences, target_term):
    G = nx.Graph()

    for word, freq in co_occurrences.items():
        G.add_node(target_term, size=50)  # Add the target term as a node
        G.add_node(word, size=freq)  # Add the co-occurring word
        G.add_edge(target_term, word, weight=freq)  # Add an edge with frequency as weight

    pos = nx.spring_layout(G, k=0.8, iterations=50)

    edge_x = []
    edge_y = []
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1, color='#888'),
        hoverinfo='none',
        mode='lines'
    )

    node_x = []
    node_y = []
    node_size = []
    node_text = []
    for node in G.nodes(data=True):
        x, y = pos[node[0]]
        node_x.append(x)
        node_y.append(y)
        node_size.append(node[1].get('size', 10) / 2)  # Scale down sizes
        node_text.append(node[0])

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            size=node_size,
            color='#FFA500',
            line_width=2
        ),
        text=node_text
    )

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title=f"Network Graph of '{target_term}' Co-occurrences",
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=0, l=0, r=0, t=50),
                        xaxis=dict(showgrid=False, zeroline=False),
                        yaxis=dict(showgrid=False, zeroline=False)
                    ))
    fig.show()

# Generate and display a word cloud
def generate_wordcloud(co_occurrences, target_term):
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

# Main function
def main():
    target_term = "playa"  # Replace with your landscape term
    window_size = 3

    # Fetch relevant tweets
    print(f"Fetching relevant tweets containing '{target_term}'...")
    tweets = fetch_tweets_with_term(engine, target_term)

    if tweets.empty:
        print(f"No relevant tweets found for '{target_term}'.")
        return

    # Count co-occurrences
    print(f"Counting co-occurrences around '{target_term}'...")
    co_occurrences, total_word_count = count_co_occurrences(tweets, target_term, window_size)

    # Generate visualizations
    print("Generating network graph...")
    generate_network_graph(co_occurrences, target_term)

    print("Generating word cloud...")
    generate_wordcloud(co_occurrences, target_term)

if __name__ == "__main__":
    main()
