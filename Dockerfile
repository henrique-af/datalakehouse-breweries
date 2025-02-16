# Use the official Apache Airflow image as the base
FROM apache/airflow:2.7.3

USER root

# Install OpenJDK 11 for Spark components
RUN apt-get update && apt-get install -y openjdk-11-jdk ant wget && apt-get clean

# Set the JAVA_HOME environment variable for Java 11
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME  # This line is unnecessary; ENV already sets the variable

# Create a directory to store additional JAR files required for Spark and S3 storage
RUN mkdir -p /opt/spark/jars

# Download Hadoop AWS connector, AWS SDK, and Spark Hadoop Cloud for S3 integration
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/3.3.2/spark-hadoop-cloud_2.12-3.3.2.jar -P /opt/spark/jars/

# Download PostgreSQL JDBC driver for Spark to interact with PostgreSQL
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -P /opt/spark/jars/

USER airflow

# Copy the Python dependencies file
COPY requirements.txt /requirements.txt

# Ensure the Airflow plugins directory is in PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/plugins"

# Upgrade pip to the latest version
RUN pip install --user --upgrade pip

# Install Python dependencies without cache to reduce image size
RUN pip install --no-cache-dir --user -r /requirements.txt