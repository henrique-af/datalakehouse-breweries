from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from minio import Minio
from minio.commonconfig import CopySource
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, trim, lit, coalesce


class GetLatestFileFromBronzeOperator(BaseOperator):
    """
    Identifies the most recent file in the Bronze bucket.
    """
    @apply_defaults
    def __init__(
        self,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        source_bucket,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with MinIO connection details and source bucket.
        :param minio_endpoint: Host and port of the MinIO service.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        :param source_bucket: Name of the bucket containing the JSON files.
        """
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.source_bucket = source_bucket

    def execute(self, context):
        """
        Identifies the most recent file in the Bronze bucket using MinIO.
        Pushes the filename to XCom for use by downstream tasks.
        :param context: Airflow context dictionary.
        :return: The name of the most recent file or None if no files are found.
        """
        # Initialize MinIO client
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        # List all objects in the source bucket, ignoring the 'processed/' folder
        objects = client.list_objects(self.source_bucket, recursive=True)
        most_recent_object = None

        # Iterate through the objects to find the most recent one
        for obj in objects:
            # Ignore the 'processed/' folder and its contents
            if not obj.object_name.startswith("processed/"):
                if not most_recent_object or obj.last_modified > most_recent_object.last_modified:
                    most_recent_object = obj

        # Extract the filename of the most recent object
        most_recent_file = most_recent_object.object_name if most_recent_object else None

        # Push the filename
        context['ti'].xcom_push(key='most_recent_file', value=most_recent_file)

        return most_recent_file


class TransformBronzeToSilverOperator(BaseOperator):
    """
    Uses Spark to transform raw JSON data in the Bronze layer into a Parquet-based Silver layer.
    - Reads JSON data from Bronze
    - Applies cleaning / transformation
    - Writes partitioned Parquet data to Silver
    """
    @apply_defaults
    def __init__(
        self,
        source_bucket,
        destination_bucket,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with MinIO connection details and bucket names.
        :param source_bucket: Name of the bucket containing raw JSON data (Bronze layer).
        :param destination_bucket: Name of the bucket to store the transformed data (Silver layer).
        :param minio_endpoint: Host and port of the MinIO service.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        """
        super().__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key

    def execute(self, context):
        """
        Main routine:
        1) Initializes Spark
        2) Identifies the most recent JSON file in the Bronze bucket
        3) Reads the JSON file
        4) Transforms the data into a standardized schema
        5) Writes the result in Parquet, partitioned by 'state'
        :param context: Airflow context dictionary.
        """
        self.log.info("Processing data from Bronze to Silver layer")

        # Initialize Spark session
        spark = self._get_spark_session()
        
        try:
            # Identify the most recent file in the Bronze bucket
            most_recent_file = context['ti'].xcom_pull(key='most_recent_file', task_ids='get_latest_file')
            if not most_recent_file:
                raise FileNotFoundError("No files found in the Bronze bucket.")

            self.log.info(f"Reading the most recent file: {most_recent_file}")

            # Read data from the most recent JSON file
            bronze_data = spark.read.json(f"s3a://{self.source_bucket}/{most_recent_file}")

            # Transform the JSON data into a curated Silver format
            silver_data = self._transform_to_silver(bronze_data)
            
            # Write data to the Silver bucket as Parquet, partitioned by 'state'
            silver_path = f"s3a://{self.destination_bucket}/silver_data"
            silver_data.write.partitionBy("state").mode("overwrite").parquet(silver_path)
            
            self.log.info(f"Silver data written to {silver_path}")

        finally:
            # Stop the Spark session
            spark.stop()
            
    def _get_most_recent_file(self):
        """
        Identifies the most recent file in the Bronze bucket using MinIO.
        Returns the name of the most recent file or None if no files are found.
        """
        
        # Initialize MinIO client
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        # List all objects in the source bucket
        objects = client.list_objects(self.source_bucket, recursive=True)

        # Find the most recent object based on last_modified timestamp
        most_recent_object = None
        for obj in objects:
            if not most_recent_object or obj.last_modified > most_recent_object.last_modified:
                most_recent_object = obj

        # Return the filename of the most recent object
        return most_recent_object.object_name if most_recent_object else None

    def _get_spark_session(self):
        """
        Builds/returns a SparkSession configured to read/write from an S3-compatible 
        storage (MinIO) using Hadoop AWS library.
        """
        return (
            SparkSession.builder
            .appName("ProcessSilverLayer")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
            )

    @staticmethod
    def _transform_to_silver(df):
        """
        Defines how to transform the raw DataFrame:
         - Renames 'name' to 'brewery_name'
         - Converts 'brewery_type' to lowercase
         - Trims 'city', merges address into 'full_address'
         - Casts longitude/latitude to double
        """
        return df.select(
            col("id"),
            trim(col("name")).alias("brewery_name"),
            lower(col("brewery_type")).alias("brewery_type"),
            concat_ws(", ", col("address_1"), col("address_2"), col("address_3")).alias("full_address"),
            trim(col("city")).alias("city"),
            col("state_province").alias("state"),
            col("country"),
            col("longitude").cast("double"),
            col("latitude").cast("double")
        )
        
class MoveFileToProcessedOperator(BaseOperator):
    """
    Moves the processed JSON file to a 'processed' folder in the Bronze bucket.
    """
    @apply_defaults
    def __init__(
        self,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        source_bucket,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with MinIO connection details and the source bucket.
        :param minio_endpoint: Host and port of the MinIO service.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        :param source_bucket: Name of the bucket containing the JSON files.
        """
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.source_bucket = source_bucket

    def execute(self, context):
        """
        Moves the processed JSON file to a 'processed' folder in the Bronze bucket.
        Creates the 'processed' folder if it doesn't exist.
        :param context: Airflow context dictionary.
        """
        # Initialize MinIO client
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        # Get the filename from XCom
        file_name = context['ti'].xcom_pull(key='most_recent_file', task_ids='get_latest_file')

        # Define source and destination paths
        source_path = file_name
        destination_path = f"processed/{file_name}"

        # Check if the 'processed' folder exists, create it if not
        processed_folder_exists = False
        for obj in client.list_objects(self.source_bucket, prefix="processed/", recursive=False):
            if obj.object_name == "processed/":
                processed_folder_exists = True
                break

        if not processed_folder_exists:
            client.put_object(self.source_bucket, "processed/", BytesIO(b""), 0)
            self.log.info("Created 'processed' folder in the Bronze bucket.")

        # Move the file
        try:
            # Use CopySource to specify the source object
            source = CopySource(self.source_bucket, source_path)
            client.copy_object(
                self.source_bucket,
                destination_path,
                source
            )
            client.remove_object(self.source_bucket, source_path)
            self.log.info(f"Moved {source_path} to {destination_path}")
        except Exception as e:
            self.log.error(f"Failed to move file: {e}")
            raise