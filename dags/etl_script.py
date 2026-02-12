import pandas as pd
import boto3
from sqlalchemy import create_engine
import os
from datetime import datetime

# Configuration
S3_BUCKET = 'sales_bucket'
S3_FILE_KEY = 'sales_data/daily_sales.csv'
LOCAL_FILE_PATH = '/opt/airflow/data/raw_sales.csv'
DB_CONNECTION_STR = 'postgresql://airflow:airflow@postgres:5432/airflow' # Using the same DB for demo

def extract_data():
    """Task 2: Connect to S3 and download data"""
    try:
        # NOTE: In a real test, ensure you have AWS credentials set in environment variables
        # s3 = boto3.client('s3')
        # s3.download_file(S3_BUCKET, S3_FILE_KEY, LOCAL_FILE_PATH)
        
        # MOCKING EXTRACTION for the test if you don't have real S3 keys:
        print("Mocking S3 download...")
        # Create a dummy CSV for demonstration
        data = {
            'product_id': [1, 2, 1, 3, None],
            'category': ['Electronics', 'Home', 'Electronics', 'Books', 'Home'],
            'amount': [100.0, 50.0, 120.0, 15.0, 60.0],
            'date': ['2023-10-01', '2023-10-01', '2023-10-01', '2023-10-01', '2023-10-01']
        }
        df = pd.DataFrame(data)
        df.to_csv(LOCAL_FILE_PATH, index=False)
        print(f"Data extracted to {LOCAL_FILE_PATH}")

    except Exception as e:
        print(f"Error extracting data: {e}")
        raise e

def transform_data():
    """Task 3: Clean and Aggregate"""
    try:
        df = pd.read_csv(LOCAL_FILE_PATH)
        
        # 1. Handling missing values (Drop rows where product_id is missing)
        df.dropna(subset=['product_id'], inplace=True)
        
        # 2. Standardize formats (ensure amount is float)
        df['amount'] = df['amount'].astype(float)
        
        # 3. Aggregate by category
        agg_df = df.groupby('category')['amount'].sum().reset_index()
        agg_df.rename(columns={'amount': 'total_sales'}, inplace=True)
        
        # Save transformed data
        output_path = '/opt/airflow/data/transformed_sales.csv'
        agg_df.to_csv(output_path, index=False)
        print(f"Data transformed and saved to {output_path}")
        
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise e

def load_data():
    """Task 4: Load to Data Warehouse (Postgres)"""
    try:
        df = pd.read_csv('/opt/airflow/data/transformed_sales.csv')
        
        # Connect to Postgres
        engine = create_engine(DB_CONNECTION_STR)
        
        # Write to table 'daily_category_sales'
        df.to_sql('daily_category_sales', engine, if_exists='replace', index=False)
        print("Data successfully loaded to Postgres")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise e