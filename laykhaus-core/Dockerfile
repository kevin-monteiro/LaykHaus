FROM python:3.12-slim

# Install system dependencies including Java for PySpark
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Upgrade pip to avoid hash issues and install Python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Download PostgreSQL JDBC and Kafka drivers for Spark
RUN mkdir -p /app/jars && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /app/jars/postgresql-42.7.1.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar \
    -o /app/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar \
    -o /app/jars/kafka-clients-3.5.0.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /app/jars/commons-pool2-2.11.1.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
    -o /app/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar

# Set Spark to use the drivers
ENV SPARK_CLASSPATH=/app/jars/*

# Copy application code
COPY ./src ./src

# Set Python path
ENV PYTHONPATH=/app/src

# Expose ports
EXPOSE 8000

# Run application
CMD ["uvicorn", "laykhaus.app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]