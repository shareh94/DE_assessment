from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
from sqlalchemy import create_engine

# --- CONFIGURATION ---
LOCAL_FILE_PATH = '/opt/airflow/data/raw_sales.csv'
TRANSFORMED_FILE_PATH = '/opt/airflow/data/transformed_sales.csv'
# Using the Airflow metadata DB for demonstration
DB_CONNECTION_STR = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

# --- PYTHON FUNCTIONS ---

def extract_data(**kwargs):
    """Task 2: Simulate extracting data from S3"""
    print("Starting extraction...")
    # Mock data creation since we don't have real S3 keys
    data = {
        'product_id': [101, 102, 101, 103, None],
        'category': ['Electronics', 'Home', 'Electronics', 'Books', 'Home'],
        'amount': [100.0, 50.0, 120.0, 15.0, 60.0],
        'date': ['2023-10-01', '2023-10-01', '2023-10-01', '2023-10-01', '2023-10-01']
    }
    df = pd.DataFrame(data)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
    
    df.to_csv(LOCAL_FILE_PATH, index=False)
    print(f"Data extracted to {LOCAL_FILE_PATH}")

def transform_data(**kwargs):
    """Task 3: Clean and Aggregate"""
    print("Starting transformation...")
    # Read the file created by the extract task
    df = pd.read_csv(LOCAL_FILE_PATH)
    
    # 1. Clean: Drop rows where product_id is missing
    print(f"Original row count: {len(df)}")
    df.dropna(subset=['product_id'], inplace=True)
    print(f"Cleaned row count: {len(df)}")
    
    # 2. Format: Ensure amount is float
    df['amount'] = df['amount'].astype(float)
    
    # 3. Aggregate: Sum sales by category
    agg_df = df.groupby('category')['amount'].sum().reset_index()
    agg_df.rename(columns={'amount': 'total_sales'}, inplace=True)
    
    # Save transformed data
    agg_df.to_csv(TRANSFORMED_FILE_PATH, index=False)
    print(f"Data transformed and saved to {TRANSFORMED_FILE_PATH}")

def check_quality(**kwargs):
    """Task 5: Data Quality Check"""
    print("Checking data quality...")
    if not os.path.exists(TRANSFORMED_FILE_PATH):
        raise FileNotFoundError("Transformed file not found!")
        
    df = pd.read_csv(TRANSFORMED_FILE_PATH)
    if df.empty:
        raise ValueError("Data Quality Check Failed: Transformed data is empty")
    
    print(f"Quality Check Passed: {len(df)} rows found.")

def load_data(**kwargs):
    """Task 4: Load to Postgres"""
    print("Loading data to database...")
    df = pd.read_csv(TRANSFORMED_FILE_PATH)
    
    # Connect to Postgres using SQLAlchemy
    engine = create_engine(DB_CONNECTION_STR)
    
    # Write to table 'daily_category_sales'
    # if_exists='replace' drops the table if it exists and creates a new one
    df.to_sql('daily_category_sales', engine, if_exists='replace', index=False)
    print("Data successfully loaded to Postgres table 'daily_category_sales'")

# --- DAG DEFINITION ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline for sales data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'etl'],
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    t2_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    t3_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_quality,
    )

    t4_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Set dependencies: Extract -> Transform -> Check -> Load
    t1_extract >> t2_transform >> t3_quality_check >> t4_load