FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Install any additional system dependencies
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  curl \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Define versions
ENV SPARK_VERSION=3.5.3
ENV SCALA_VERSION=2.12

# Add Spark Kafka connector and dependencies
RUN cd /usr/local/spark && \
  wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar -P jars/ && \
  wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar -P jars/ && \
  wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P jars/ && \
  wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar -P jars/ && \
  wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar -P jars/

# Set environment variables for Spark to find the jars
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION} pyspark-shell"

# Copy the requirements file
COPY requirements.txt /tmp/

# Install Python packages
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create directory for checkpoints
RUN mkdir -p /tmp/checkpoint && \
  chown -R ${NB_UID}:${NB_GID} /tmp/checkpoint

# Switch back to the notebook user
USER ${NB_UID}

# Set working directory
WORKDIR /home/jovyan
