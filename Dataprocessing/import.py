import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="NordProJect_123"
        )
        print("Database connection established.")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None

def import_csv_to_db(csv_file_path, table_name="manual_labels", schema_name="variance"):
    # Load the CSV file
    df = pd.read_csv(csv_file_path)

    # Define connection string for SQLAlchemy
    db_connection_str = "postgresql+psycopg2://postgres:NordProJect_123@localhost/postgres"
    engine = create_engine(db_connection_str)

    # Insert data into the specified schema and table
    df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
    print("Data imported successfully into the table:", schema_name + "." + table_name)

# Usage example:
csv_file_path = "/Users/lippi/Library/Mobile Documents/com~apple~CloudDocs/04_Master/MasterThesis/Python/variance/manual_data.csv"  # Replace with the actual path to your CSV file
import_csv_to_db(csv_file_path)
