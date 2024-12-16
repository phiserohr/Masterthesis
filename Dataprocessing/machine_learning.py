import pandas as pd
import pickle
from sqlalchemy import create_engine, text
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from tqdm import tqdm

# Database connection details
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "NordProJect_123"
DB_SCHEMA = "variance"

# Connect to database
def connect_to_db():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")
    return engine

# Load labeled data and train model
def train_and_save_model(engine):
    # Load labeled data
    query = f'SELECT "Tweet Text" AS tweet_text, "Label" FROM {DB_SCHEMA}.manual_labels'
    labeled_data = pd.read_sql(text(query), engine)
    
    # Prepare training data with progress tracking
    vectorizer = TfidfVectorizer()
    print("Vectorizing training data...")
    X_train = vectorizer.fit_transform(tqdm(labeled_data['tweet_text'].fillna('')))
    y_train = labeled_data['Label']
    
    # Define model and parameters for GridSearch
    model = RandomForestClassifier(random_state=42)
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [None, 10, 20, 30],
        'min_samples_split': [2, 5, 10]
    }
    
    print("Starting GridSearch for best model parameters...")
    grid_search = GridSearchCV(model, param_grid, cv=3, scoring='accuracy', verbose=1)
    grid_search.fit(X_train, y_train)
    
    # Get the best model and fit it
    best_model = grid_search.best_estimator_
    
    # Save the vectorizer and model as .pkl files
    with open("tfidf_vectorizer.pkl", "wb") as vec_file:
        pickle.dump(vectorizer, vec_file)
    with open("best_random_forest_model.pkl", "wb") as model_file:
        pickle.dump(best_model, model_file)
    
    print("Model and vectorizer saved as 'best_random_forest_model.pkl' and 'tfidf_vectorizer.pkl'.")
    
    return best_model, vectorizer

# Predict labels for all filtered tweets
def predict_labels(engine, model, vectorizer):
    # Load all unlabeled tweets with all columns
    query = f'SELECT * FROM {DB_SCHEMA}.filtered_tweets'
    unlabeled_data = pd.read_sql(text(query), engine)
    
    # Vectorize tweet text and predict labels with progress tracking
    print(f"Vectorizing {len(unlabeled_data)} tweets for prediction...")
    X_unlabeled = vectorizer.transform(tqdm(unlabeled_data['tweet_text'].fillna('')))
    
    print("Predicting labels...")
    predictions = model.predict(X_unlabeled)
    
    # Add predictions to the DataFrame
    unlabeled_data['Label'] = predictions
    return unlabeled_data

# Save predictions to a new table
def save_predictions(engine, predicted_data):
    predicted_data.to_sql("labeled_tweets", engine, schema=DB_SCHEMA, if_exists="replace", index=False)
    print("Predictions saved to 'labeled_tweets' table.")

def main():
    engine = connect_to_db()
    
    # Train the model and save it as a .pkl file
    model, vectorizer = train_and_save_model(engine)
    
    # Predict labels for all tweets
    predicted_data_full = predict_labels(engine, model, vectorizer)
    save_predictions(engine, predicted_data_full)
    
    engine.dispose()
    print("Finished labeling all available tweets.")

if __name__ == "__main__":
    main()
