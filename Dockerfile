# Use the official Apache Airflow image
FROM apache/airflow:2.7.1
# FROM apache/airflow:2.7.2-python3.9

# Switch to root user to install dependencies
USER root

# Set the working directory
WORKDIR /PROJECT

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Switch back to airflow user
USER airflow

# Install dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Switch back to root user to install Spark
USER root

# Install Spark
RUN curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH="$SPARK_HOME/bin:$PATH"

# Rename the Excel file to avoid spaces
# COPY AdventureWorks_Sales.xlsx /opt/airflow/excel/

# Copy Airflow configuration file
COPY airflow.cfg /opt/airflow/airflow.cfg

# Copy the ETL and DAGs directories
COPY etl /opt/airflow/dags/etl
COPY dags /opt/airflow/dags

# Copy the SQL file
# COPY etl/s3_sf_data.py /opt/airflow/etl/s3_sf_data.py

# Copy the new Python script
COPY etl/ETL_S3_SNOWFLAKES.py /opt/airflow/etl/ETL_S3_SNOWFLAKES.py

# Copy the credentials file
# COPY etl/credentials.py /opt/airflow/etl/credentials.py
COPY .env /opt/airflow/.env
# Expose the port for the webserver
EXPOSE 8080

# Switch back to airflow user
USER airflow

# Use the airflow standalone command to initialize the database, create an admin user, and start the webserver and scheduler
CMD ["bash", "-c", "airflow db init && airflow db upgrade && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin && airflow standalone"]