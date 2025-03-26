from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, lit, current_timestamp, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

spark = (
    SparkSession.builder.appName("NYC Taxi Data Kafka ETL")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# noise reduction
spark.sparkContext.setLogLevel("WARN")

taxi_schema = StructType(
    [
        StructField("medallion", StringType(), True),
        StructField("hack_license", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("trip_time_in_secs", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
    ]
)


def ingest_csv_to_kafka(csv_path, batch_size=1000):
    """
    Read a CSV file in batches and publish records to Kafka
    """
    print(
        f"Ingesting data from {csv_path} to Kafka topic 'raw-taxi-data' with batch size {batch_size}"
    )

    # loading the data
    taxi_data = spark.read.csv(csv_path, header=True, schema=taxi_schema)

    # Get total count for progress reporting
    total_count = taxi_data.count()
    print(f"Total records to process: {total_count}")

    # Process in batches
    for offset in range(0, total_count, batch_size):
        current_batch = taxi_data.limit(batch_size).offset(offset)

        # medallion used as partition key
        current_batch = current_batch.withColumn("kafka_key", col("medallion"))

        # jsonify the data for kafka
        batch_json = current_batch.select(
            col("kafka_key").cast("string"),
            to_json(
                struct(*[col(c) for c in current_batch.columns if c != "kafka_key"])
            ).alias("value"),
        )

        # write current batch
        batch_json.write.format("kafka").option(
            "kafka.bootstrap.servers", "kafka:9092"
        ).option("topic", "raw-taxi-data").save()

        print(
            f"Progress: processed {min(offset + batch_size, total_count)}/{total_count} records"
        )

    print(f"Finished ingesting data to Kafka topic 'raw-taxi-data'")
