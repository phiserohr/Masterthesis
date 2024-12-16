import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

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

# Load labeled data from the database
def load_labeled_data(engine):
    query = f'SELECT "Tweet Text" AS tweet_text, "Label" FROM {DB_SCHEMA}.manual_labels'
    labeled_data = pd.read_sql(text(query), engine)
    return labeled_data

# Evaluate model accuracy on manually labeled data
def evaluate_model(labeled_data):
    # Split data into training and testing sets
    X = labeled_data['tweet_text'].fillna('')
    y = labeled_data['Label']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Vectorize tweet text
    vectorizer = TfidfVectorizer()
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)
    
    # Train the model
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train_vec, y_train)
    
    # Predict on the test set
    y_pred = model.predict(X_test_vec)
    
    # Calculate evaluation metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")

def main():
    engine = connect_to_db()
    
    # Load labeled data
    labeled_data = load_labeled_data(engine)
    
    # Evaluate model on labeled data
    evaluate_model(labeled_data)
    
    engine.dispose()

if __name__ == "__main__":
    main()
