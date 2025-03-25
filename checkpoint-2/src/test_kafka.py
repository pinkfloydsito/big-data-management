from pyspark.sql import SparkSession

"""
in order to check this script you need to enter in the docker container and execute the file manually.
check the kafka UI to see the topic dequeueing the messages and check the spark ui as well 
to see the processing.
"""

# session
spark = SparkSession.builder.appName("KafkaTest").getOrCreate()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "test-topic")
    .load()
)

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# just output in the console
query = df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
