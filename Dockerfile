FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-1.17.0-openjdk-amd64

# Copy requirements.txt file to install additional Python dependencies
COPY requirements.txt /requirements.txt

USER airflow

# Install pip dependencies as 'airflow' user
RUN pip install --upgrade pip && pip install --no-cache-dir -r /requirements.txt

# Install additional dependencies
RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow-providers-apache-spark \
    pyspark \
    delta-spark \
    duckdb \
    duckdb-engine