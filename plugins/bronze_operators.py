from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from minio import Minio
from io import BytesIO
import json
from datetime import datetime

class EnsureBucketsExistOperator(BaseOperator):
    """
    Ensures that specified MinIO buckets exist.
    """
    @apply_defaults
    def __init__(
        self,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        buckets,  # Pass a list of bucket names
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.buckets = buckets

    def execute(self, context):
        """
        Checks if buckets exist and creates them if they don't.
        """
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        for bucket in self.buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                self.log.info(f"Bucket '{bucket}' created")
            else:
                self.log.info(f"Bucket '{bucket}' already exists")


class WriteDataToBronzeOperator(BaseOperator):
    """
    Writes raw data in JSON format to a Bronze layer bucket on MinIO. Also ensures that bronze, silver and gold buckets exists.
    """
    @apply_defaults
    def __init__(
        self,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        bronze_bucket,
        data_key=None,
        filename_prefix='data',
        *args,
        **kwargs
    ):
        """
        :param bucket: The name of the MinIO bucket for storing raw data.
        :param minio_endpoint: Host and port of the MinIO service (e.g., "minio:9000").
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        :param data_key: (Optional) XCom key to pull data from; if None, pulls from default.
        :param filename_prefix: (Optional) Prefix for naming the stored file.
        """
        super().__init__(*args, **kwargs)
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.bronze_bucket = bronze_bucket
        self.data_key = data_key
        self.filename_prefix = filename_prefix

    def execute(self, context):
        """
        Retrieves data from XCom, converts it to JSON, and stores it in the specified
        MinIO bucket. Creates the bucket if it does not exist.
        """
        self.log.info(f"Writing data to Bronze layer: {self.bronze_bucket}")

        if self.data_key:
            data = context['ti'].xcom_pull(key=self.data_key)
        else:
            data = context['ti'].xcom_pull()

        # Initialize the MinIO client
        client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        # Convert the Python data to JSON and store in MinIO
        data_json = json.dumps(data)
        data_bytes = data_json.encode('utf-8')
        data_file = BytesIO(data_bytes)

        # Generate a timestamped filename
        filename = f"{self.filename_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        client.put_object(
            self.bronze_bucket, 
            filename, 
            data_file, 
            length=len(data_bytes),
            content_type='application/json'
        )

        self.log.info(f"Data stored in MinIO bucket '{self.bronze_bucket}' as '{filename}'")
        return filename