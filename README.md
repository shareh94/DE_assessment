# Daily Sales ETL Pipeline

This project implements a Data Engineering pipeline to automate the extraction, transformation, and loading (ETL) of daily sales data using **Apache Airflow**, **Docker**, and **Python**.

##Project Overview

The pipeline is designed to:
1.  **Extract:** Download daily sales CSV files (mocked S3 download).
2.  **Transform:** Clean data (remove nulls, standardize types) and aggregate sales by category.
3.  **Validate:** Perform data quality checks to ensure data integrity.
4.  **Load:** Load the aggregated results into a PostgreSQL data warehouse.

##Tech Stack & Architecture

* **Containerization:** Docker & Docker Compose
* **Orchestration:** Apache Airflow (2.7.1)
* **Data Warehouse:** PostgreSQL (13)
* **Transformation:** Python (Pandas)
* **Testing:** Python `unittest`
