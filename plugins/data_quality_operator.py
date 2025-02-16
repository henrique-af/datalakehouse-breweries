from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

class DataQualityOperator(BaseOperator):
    """
    Checks data quality conditions (e.g., non-empty fields, minimum record count) 
    on a DataFrame loaded from Parquet in MinIO.
    Fails the task if conditions are not met.
    """

    @apply_defaults
    def __init__(
        self,
        source_bucket: str,
        minio_endpoint: str,
        minio_access_key: str,
        minio_secret_key: str,
        min_record_count: int = 50,
        critical_columns: list = None,
        *args, 
        **kwargs
    ):
        """
        :param source_bucket: Bucket name where Parquet data resides.
        :param minio_endpoint: MinIO endpoint host:port.
        :param minio_access_key: Access key for MinIO.
        :param minio_secret_key: Secret key for MinIO.
        :param min_record_count: Minimum records expected (default=50).
        :param critical_columns: List of column names that must not be null.
        """
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.min_record_count = min_record_count
        self.critical_columns = critical_columns or []

    def execute(self, context):
        # 1) Initialize Spark
        spark = (
            SparkSession.builder
            .appName("DataQualityCheck")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )

        try:
            # 2) Read the Parquet data from MinIO
            path = f"s3a://{self.source_bucket}/silver_data"
            df = spark.read.parquet(path)

            # 3) Check if DataFrame is empty
            if df.isEmpty():
                raise ValueError(f"Data quality check failed: {path} is empty.")
            
            # 4) Check for minimum records
            if df.limit(self.min_record_count).count() < self.min_record_count:
                raise ValueError(f"Data quality check failed: record count is below threshold {self.min_record_count}.")
            
            # 5) Check for nulls in critical columns
            for col_name in self.critical_columns:
                if df.filter(col(col_name).isNull() | (col(col_name) == "")).limit(1).count() > 0:
                    raise ValueError(f"Data quality check failed: column '{col_name}' contains null or empty values.")

            self.log.info("Data quality checks passed successfully.")
        finally:
            spark.stop()