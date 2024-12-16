import pandas as pd
import pickle
from sqlalchemy import create_engine, text
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "*********"
DB_SCHEMA = "variance"

# Connect to DB
def connect_to_db():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Load Data
def load_labeled_data(engine):
    query = f'SELECT "Tweet Text" AS tweet_text, "Label" FROM {DB_SCHEMA}.manual_labels'
    return pd.read_sql(text(query), engine)

# Load Model
def load_model_and_vectorizer():
    with open("best_random_forest_model.pkl", "rb") as model_file:
        model = pickle.load(model_file)
    with open("tfidf_vectorizer.pkl", "rb") as vec_file:
        vectorizer = pickle.load(vec_file)
    return model, vectorizer

# Evaluate Model
def evaluate_model(model, vectorizer, labeled_data):
    X_test = labeled_data['tweet_text'].fillna('')
    y_test = labeled_data['Label']
    X_test_vec = vectorizer.transform(X_test)
    y_pred = model.predict(X_test_vec)
    print(classification_report(y_test, y_pred))

def main():
    engine = connect_to_db()
    try:
        labeled_data = load_labeled_data(engine)
        model, vectorizer = load_model_and_vectorizer()
        evaluate_model(model, vectorizer, labeled_data)
    finally:
        engine.dispose()
if __name__ == "__main__":
    main()
