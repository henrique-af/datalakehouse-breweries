"""
Brewery Data Pipeline DAG

This DAG orchestrates a data pipeline using the Medallion Architecture:
1. Bronze layer: Raw data from OpenBreweryDB API
2. Silver layer: Transformed data in Parquet format, partitioned by location
3. Gold layer: Aggregated data stored in PostgreSQL

The pipeline includes data extraction, transformation, quality checks, and storage
across MinIO buckets and a PostgreSQL database.
"""

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import os
from bronze_operators import WriteDataToBronzeOperator, EnsureBucketsExistOperator
from silver_operators import GetLatestFileFromBronzeOperator, TransformBronzeToSilverOperator, MoveFileToProcessedOperator
from gold_operators import EnsurePostgresSchemaOperator, AggregateSilverToGoldOperator, WriteGoldToPostgresOperator
from brewery_operators import APIExtractorOperator
from data_quality_operator import DataQualityOperator

# Configuration
API_URL = os.getenv('BREWERY_API_URL', "https://api.openbrewerydb.org/breweries")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minio')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio123')

BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'
GOLD_BUCKET = 'gold'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres-analytics')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', 5432)
POSTGRES_DB = os.getenv('POSTGRES_DB', 'brewery_analytics')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'analytics')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'analytics123')

# DAG default arguments
default_args = {
    "owner": "Henrique",
    "start_date": datetime(2024, 10, 1),
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "max_active_runs": 1,
    "retries": 3,
}

# DAG definition
with DAG(
    dag_id="brewery_API_pipeline",
    default_args=default_args,
    description="DAG to process brewery data through Bronze, Silver, and Gold layers",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Task 1: Check API availability
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="brewery_api",
        endpoint="/breweries",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        timeout=30,
        poke_interval=10,
        mode="poke",
    )

    # Task 2: Ensure MinIO buckets exist
    ensure_buckets = EnsureBucketsExistOperator(
        task_id='ensure_buckets',
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        buckets=[BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET]
    )
    
    # Task 3: Extract data from API
    extract_data = APIExtractorOperator(
        task_id='extract_data',
        api_url=API_URL
    )

    # Task 4: Write data to Bronze layer
    write_to_bronze = WriteDataToBronzeOperator(
        task_id='write_to_bronze',
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        bronze_bucket=BRONZE_BUCKET,
        data_key=None
    )
    
    # Task 5: Get latest file from Bronze layer
    get_latest_file = GetLatestFileFromBronzeOperator(
        task_id='get_latest_file',
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        source_bucket=BRONZE_BUCKET
    )

    # Task 6: Transform Bronze to Silver
    transform_to_silver = TransformBronzeToSilverOperator(
        task_id='transform_to_silver',
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        source_bucket=BRONZE_BUCKET,
        destination_bucket=SILVER_BUCKET
    )
    
    # Task 7: Perform data quality check
    data_quality_check = DataQualityOperator(
        task_id='data_quality_check',
        source_bucket=SILVER_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        min_record_count=50, 
        critical_columns=['brewery_name', 'city', 'state']
    )

    # Task 8: Move processed file to 'processed' folder
    move_to_processed = MoveFileToProcessedOperator(
        task_id='move_to_processed',
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        source_bucket=BRONZE_BUCKET
    )
    
    # Task 9: Ensure PostgreSQL schema exists
    ensure_postgres_schema = EnsurePostgresSchemaOperator(
        task_id='ensure_postgres_schema',
        pg_host=POSTGRES_HOST,
        pg_port=POSTGRES_PORT,
        pg_db=POSTGRES_DB,
        pg_user=POSTGRES_USER,
        pg_password=POSTGRES_PASSWORD,
        schema_name='gold_layer'
    )

    # Task 10: Aggregate Silver to Gold
    aggregate_silver_to_gold = AggregateSilverToGoldOperator(
        task_id='aggregate_silver_to_gold',
        source_bucket=SILVER_BUCKET,
        destination_bucket=GOLD_BUCKET,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY
    )

    # Task 11: Write Gold data to PostgreSQL
    write_gold_to_postgres = WriteGoldToPostgresOperator(
        task_id='write_gold_to_postgres',
        pg_host=POSTGRES_HOST,
        pg_port=POSTGRES_PORT,
        pg_db=POSTGRES_DB,
        pg_user=POSTGRES_USER,
        pg_password=POSTGRES_PASSWORD,
        minio_endpoint=MINIO_ENDPOINT,
        minio_access_key=MINIO_ACCESS_KEY,
        minio_secret_key=MINIO_SECRET_KEY,
        source_bucket=GOLD_BUCKET
    )

# Define task dependencies
check_api >> ensure_buckets >> extract_data >> write_to_bronze >> get_latest_file >> transform_to_silver >> data_quality_check >> move_to_processed >> ensure_postgres_schema >> aggregate_silver_to_gold >> write_gold_to_postgres