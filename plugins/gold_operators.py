from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

class EnsurePostgresSchemaOperator(BaseOperator):
    """
    Ensures that a schema exists in PostgreSQL using psycopg2.
    """
    @apply_defaults
    def __init__(
        self,
        pg_host,
        pg_port,
        pg_db,
        pg_user,
        pg_password,
        schema_name,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with PostgreSQL connection details and the schema name.
        :param pg_host: Hostname for PostgreSQL server.
        :param pg_port: Port for PostgreSQL.
        :param pg_db: Database name in PostgreSQL.
        :param pg_user: PostgreSQL user.
        :param pg_password: PostgreSQL password.
        :param schema_name: The name of the schema to ensure exists.
        """
        super().__init__(*args, **kwargs)
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.schema_name = schema_name

    def execute(self, context):
        """
        Connects to PostgreSQL using psycopg2 and creates the schema if it doesn't exist.
        """
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                dbname=self.pg_db,
                user=self.pg_user,
                password=self.pg_password
            )
            # Set autocommit to True to execute CREATE SCHEMA statement
            conn.set_session(autocommit=True)
            cur = conn.cursor()

            # Create the schema if it doesn't exist
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name};")

            # Close the cursor and connection
            cur.close()
            conn.close()

            self.log.info(f"Schema '{self.schema_name}' ensured in PostgreSQL.")
        except Exception as e:
            self.log.error(f"Error creating schema {self.schema_name}: {e}")
            raise


class AggregateSilverToGoldOperator(BaseOperator):
    """
    Reads the Silver data with Spark, aggregates info into the Gold layer,
    and writes the aggregated data to MinIO.
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
        :param source_bucket: Bucket to read the Silver Parquet data from.
        :param destination_bucket: Bucket to write the aggregated Gold Parquet data to.
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
        Main logic:
         1) Read Silver data from MinIO (Parquet).
         2) Aggregate data into a Gold DataFrame.
         3) Write the Gold data to MinIO (Parquet).
        """
        # Initialize Spark session
        spark = self._get_spark_session(self.minio_endpoint, self.minio_access_key, self.minio_secret_key)
        try:
            # Define the path to the Silver data
            silver_path = f"s3a://{self.source_bucket}/silver_data"

            # Read the Silver data into a DataFrame
            df_silver = spark.read.parquet(silver_path)

            # Create the Gold view by aggregating the Silver data
            df_gold = self._create_gold_view(df_silver)

            # Define the path to write the Gold data
            gold_path = f"s3a://{self.destination_bucket}/gold_data"

            # Write the Gold data to MinIO in Parquet format
            df_gold.write.mode("overwrite").parquet(gold_path)
            self.log.info(f"Gold data written to {gold_path}")

            # Push the gold data to XCom for the next task
            context['ti'].xcom_push(key='gold_data', value=gold_path)

        finally:
            # Stop the Spark session
            spark.stop()

    def _get_spark_session(self, minio_endpoint, minio_access_key, minio_secret_key):
        """
        Instantiates a SparkSession that can read from MinIO.
        """
        return (
            SparkSession.builder.appName("AggregateSilverToGold")
            .config("spark.master", "spark://spark-master:7077")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()
        )

    @staticmethod
    def _create_gold_view(df):
        """
        Aggregates the data by brewery_type and state, computing
        the total number of breweries (brewery_count).
        """
        return df.groupBy("brewery_type", "state").agg(count("id").alias("brewery_count"))


class WriteGoldToPostgresOperator(BaseOperator):
    """
    Writes the aggregated Gold data from MinIO to a PostgreSQL database.
    """
    @apply_defaults
    def __init__(
        self,
        pg_host,
        pg_port,
        pg_db,
        pg_user,
        pg_password,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        source_bucket,
        *args,
        **kwargs
    ):
        """
        Initializes the operator with PostgreSQL and MinIO connection details.
        :param source_bucket: Bucket to read the Gold Parquet data from.
        :param pg_host: Hostname for PostgreSQL server.
        :param pg_port: Port for PostgreSQL.
        :param pg_db: Database name in PostgreSQL.
        :param pg_user: PostgreSQL user.
        :param pg_password: PostgreSQL password.
        :param minio_endpoint: Host and port of the MinIO service.
        :param minio_access_key: Access key for MinIO authentication.
        :param minio_secret_key: Secret key for MinIO authentication.
        """
        super().__init__(*args, **kwargs)
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_db = pg_db
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.source_bucket = source_bucket

    def execute(self, context):
        """
        Writes the aggregated DataFrame to PostgreSQL in the table 'gold_layer.brewery_summary',
        overwriting any existing data.
        """
        # Get the gold data path from XCom
        gold_path = f"s3a://{self.source_bucket}/gold_data"

        # Initialize Spark session
        spark = self._get_spark_session(self.minio_endpoint, self.minio_access_key, self.minio_secret_key)
        try:
            # Read the Gold data from MinIO
            df_gold = spark.read.parquet(gold_path)

            # Construct the JDBC URL for PostgreSQL
            jdbc_url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

            # Write the DataFrame to PostgreSQL using JDBC
            df_gold.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "gold_layer.brewery_summary") \
                .option("user", self.pg_user) \
                .option("password", self.pg_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

            self.log.info("Gold data written to PostgreSQL in table 'gold_layer.brewery_summary'.")

        finally:
            # Stop the Spark session
            spark.stop()

    def _get_spark_session(self, minio_endpoint, minio_access_key, minio_secret_key):
        """
        Instantiates a SparkSession that can read from MinIO and write to PostgreSQL
        using the org.postgresql driver.
        """
        return (
            SparkSession.builder.appName("WriteGoldToPostgres")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.2.18")
            .config("spark.master", "spark://spark-master:7077")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_endpoint}")
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()
        )