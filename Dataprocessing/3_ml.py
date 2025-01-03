#machine learning, step 3

import pandas as pd
import pickle
from sqlalchemy import create_engine, text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import RandomizedSearchCV

# Database connection 
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "***********"
DB_SCHEMA = "variance"

# Connect DB
def connect_to_db():
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Train model
def train_model(engine, save_files=True):
    query = f'SELECT "Tweet Text" AS tweet_text, "Label" FROM {DB_SCHEMA}.manual_labels'
    labeled_data = pd.read_sql(text(query), engine)
    
    vectorizer = TfidfVectorizer()
    X_train = vectorizer.fit_transform(labeled_data['tweet_text'].fillna(''))
    y_train = labeled_data['Label']
    
    model = RandomForestClassifier(random_state=42)
    param_dist = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5]
    }
    
    grid_search = RandomizedSearchCV(model, param_distributions=param_dist, n_iter=10, cv=3, random_state=42, verbose=1)
    grid_search.fit(X_train, y_train)
    best_model = grid_search.best_estimator_
    
    if save_files:
        pickle.dump(vectorizer, open("tfidf_vectorizer.pkl", "wb"))
        pickle.dump(best_model, open("best_random_forest_model.pkl", "wb"))
    
    return best_model, vectorizer

# Predict labels
def predict_labels(engine, model, vectorizer):
    query = f'SELECT id, tweet_text FROM {DB_SCHEMA}.filtered_tweets'
    unlabeled_data = pd.read_sql(text(query), engine)
    
    X_unlabeled = vectorizer.transform(unlabeled_data['tweet_text'].fillna(''))
    predictions = model.predict(X_unlabeled)
    unlabeled_data['Label'] = predictions
    return unlabeled_data

# Save predictions
def save_predictions(engine, predicted_data):
    predicted_data.to_sql("labeled_tweets", engine, schema=DB_SCHEMA, if_exists="replace", index=False)

def main():
    engine = connect_to_db()
    model, vectorizer = train_model(engine)
    predicted_data = predict_labels(engine, model, vectorizer)
    save_predictions(engine, predicted_data)
    engine.dispose()
    print("Finished.")

if __name__ == "__main__":
    main()
