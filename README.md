**Daily Sales ETL Pipeline**

This assessment implements a Data Engineering pipeline to automate the extraction, transformation, and loading (ETL) of daily sales data using **Apache Airflow**, **Docker**, and **Python**.

**Assessment Overview**

The pipeline is designed to:
1.  **Extract:** Download daily sales CSV files (mocked S3 download).
2.  **Transform:** Clean data (remove nulls, standardize types) and aggregate sales by category.
3.  **Validate:** Perform data quality checks to ensure data integrity.
4.  **Load:** Load the aggregated results into a PostgreSQL data warehouse.

**Tech Stack & Architecture**

* **Containerization:** Docker & Docker Compose
* **Orchestration:** Apache Airflow (2.7.1)
* **Data Warehouse:** PostgreSQL (13)
* **Transformation:** Python (Pandas)
* **Testing:** Python `unittest`

**How to set up the ETL pipeline**

1) Prerequisites
- Ensure we have Docker Desktop installed and running.

2) Installation
- Clone the repository and enter the directory:
  "git clone https://github.com/shareh94/DE_assessment.git"
  "cd airflow-etl-pipeline"

3) Start the Environment
- Run the following command to download images and start the containers (Scheduler, Webserver, Postgres):
  "docker compose up -d"

4) Initialize Airflow
- Since this project uses a local database, we must initialize the Airflow metadata and create an admin user on the first run:

  Initialize the Database:
  "docker compose run webserver airflow db init"

  Create Admin User:
  "docker compose run webserver airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow"

**How to run the Pipeline**

1) Access the UI:
- Open your browser to http://localhost:8080.
  Username: airflow
  Password: airflow

2) Trigger the Job:
- Locate the DAG named sales_etl_pipeline.
- Toggle the switch on the left to Unpause the DAG.
- Click the Play Button (▶) on the right side to trigger a manual run.

3) Verify Success:
- Click on the DAG name to see the "Grid" view.
- You should see dark green squares indicating successful tasks (Extract -> Transform -> Quality Check -> Load).

**Verification & Testing**

1) Run Unit Tests
- To verify the transformation logic using the built-in unit tests:
  "docker compose exec scheduler python -m unittest tests.test_etl"

  Expected Output: "Ran 1 test in 0.00xs ... OK"

2) Verify Data in Warehouse
- To check if the data was actually loaded into the Postgres database:
  "docker compose exec postgres psql -U airflow -c "SELECT * FROM daily_category_sales;""
