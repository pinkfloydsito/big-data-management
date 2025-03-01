from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f

PROJECT_NAME = "NY-Concrete-Jungle-Analysis"

sample_path = "./sample-data.csv"
sc = SparkContext("local", PROJECT_NAME)
spark = (
    SparkSession.builder.appName(PROJECT_NAME)
    .enableHiveSupport()  # Enables Hive support, persistent Hive metastore
    .getOrCreate()
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

import pandas as pd

pd.set_option("display.max_columns", 1000)

data = pd.read_csv("sample-data.csv")

data.head()
data.columns


"""
medallion', 
'hack_license', 
'vendor_id', 
'rate_code',
'store_and_fwd_flag', 
'pickup_datetime', 
'dropoff_datetime',
'passenger_count', 
'pickup_longitude', 
'pickup_latitude',
'dropoff_longitude', 
'dropoff_latitude'
"""

data.head()

"""
id = hack_license
pickup_location: 'pickup_longitude', 'pickup_latitude'
dropoff_location: 'dropoff_longitude', 'dropoff_latitude'
pickup_datetime, dropoff_datetime
"""
schema = StructType(
    [
        StructField("medallion", StringType()),
        StructField("hack_license", StringType()),
        StructField("vendor_id", StringType()),
        StructField("rate_code", StringType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("pickup_datetime", StringType()),
        StructField("dropoff_datetime", StringType()),
        StructField("passenger_count", IntegerType()),
        StructField("pickup_longitude", DoubleType()),
        StructField("pickup_latitude", DoubleType()),
        StructField("dropoff_longitude", DoubleType()),
        StructField("dropoff_latitude", DoubleType()),
    ]
)

sample_df = (
    spark.read.option("sep", ",")  # separator
    .option("header", True)  # file has header row
    .schema(schema)  # spark tries to infer data types
    .option("dateFormat", "dd-MM-yy HH:mm")  # XXX: why is this not working?
    .csv(sample_path)
)

# sample_df.head(5)

# sample_df.select(
#     f.to_timestamp(f.col('pickup_datetime'), 'dd-MM-yy HH:mm').alias('pickup_datetime')
# ).show(5)

df = sample_df.withColumn(
    "pickup_datetime", f.to_timestamp("pickup_datetime", "dd-MM-yy HH:mm")
).withColumn("dropoff_datetime", f.to_timestamp("dropoff_datetime", "dd-MM-yy HH:mm"))

df.head(5)

df = df.select(
    "hack_license",
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
    "pickup_datetime",
    "dropoff_datetime",
)

# Show the result
df.show(5)

display(df)
df.printSchema()
"""
root
 |-- hack_license: string (nullable = true)
 |-- pickup_longitude: double (nullable = true)
 |-- pickup_latitude: double (nullable = true)
 |-- dropoff_longitude: double (nullable = true)
 |-- dropoff_latitude: double (nullable = true)
 |-- pickup_datetime: timestamp (nullable = true)
 |-- dropoff_datetime: timestamp (nullable = true)
"""


"""
First, we need to compute one important metric; utilization. Utilization is the fraction of time that a cab
is on the road and is occupied by one or more passengers. One factor that impacts utilization is the
passenger’s destination: a cab that drops off passengers near Union Square at midday is much more
likely to find its next fare in just a minute or two, whereas a cab that drops someone off at 2 AM on Staten
Island may have to drive all the way back to Manhattan before it find its next fare.
We need to write a spark programs for each of the following query:
1. Utilization: This is per taxi/driver. This can be computed by computing the idle time per
taxi. We will elaborate on that more later
"""

window_spec = Window.partitionBy("hack_license").orderBy("pickup_datetime")

# Calculate trip duration and idle time between trips
trip_data = (
    df.withColumn(
        "trip_duration_ms",
        (f.col("dropoff_datetime").cast("long") - f.col("pickup_datetime").cast("long"))
        * 1000,
    )
    .withColumn("next_pickup", f.lead("pickup_datetime").over(window_spec))
    .withColumn(
        "idle_time_ms",
        f.when(
            f.col("next_pickup").isNotNull(),
            (f.col("next_pickup").cast("long") - f.col("dropoff_datetime").cast("long"))
            * 1000,
        ).otherwise(0),
    )
)
# Calculate utilization per driver
utilization_per_driver = (
    trip_data.groupBy("hack_license")
    .agg(
        f.sum("trip_duration_ms").alias("total_trip_duration_ms"),
        f.sum("idle_time_ms").alias("total_idle_time_ms"),
    )
    .withColumn(
        "utilization_rate",
        f.round(
            f.col("total_trip_duration_ms")
            / (f.col("total_trip_duration_ms") + f.col("total_idle_time_ms")),
            4,
        ),
    )
)

# Show the results
utilization_per_driver.select(
    "hack_license", "total_trip_duration_ms", "total_idle_time_ms", "utilization_rate"
).show()


"""

+--------------------+----------------------+------------------+----------------+
|        hack_license|total_trip_duration_ms|total_idle_time_ms|utilization_rate|
+--------------------+----------------------+------------------+----------------+
|001C8AAB90AEE49F3...|               2820000|         864180000|          0.0033|
|0025133AD810DBE80...|               1440000|           2400000|           0.375|
|002C093A2CB9FD40C...|              12600000|          15300000|          0.4516|
|00374328FBA75FBFC...|                960000|                 0|             1.0|
|00447A6197DBB329F...|               6420000|          13440000|          0.3233|
|0046F1E91AA13DEDE...|              11580000|           9960000|          0.5376|
|00567B1CBFD51DDFA...|              11520000|          10080000|          0.5333|
|0057CCB5BA8D29E34...|                300000|                 0|             1.0|
|006114F940CB87B3A...|              16380000|          24000000|          0.4056|
|006313464EC98A24B...|              11280000|          31500000|          0.2637|
|006B6BD90C7B5C985...|               5520000|           6180000|          0.4718|
|00711D0CC3FB5BC90...|               3960000|          51060000|           0.072|
|007357E7FFE212879...|               9420000|          18660000|          0.3355|
|007439EEDB510EF82...|                900000|           3240000|          0.2174|
|0078BA33E03313B58...|                240000|                 0|             1.0|
|007E686365B4421FB...|               3600000|           3840000|          0.4839|
|008BE4F3FF9393504...|                360000|                 0|             1.0|
|00927C48BA4C1B2B1...|              12120000|          14460000|           0.456|
|00A2DC1380E44036A...|              10140000|          11100000|          0.4774|
|00A84F2983BCE93E9...|                840000|                 0|             1.0|
+--------------------+----------------------+------------------+----------------+
"""

import json
from shapely.geometry import Point, shape
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os

os.getcwd()

with open("./nyc-boroughs.geojson", "r") as f:
    borough_data = json.load(f)

borough_shapes = {}
for feature in borough_data["features"]:
    borough_name = feature["properties"]["borough"]
    borough_geometry = shape(feature["geometry"])
    borough_shapes[borough_name] = borough_geometry

borough_shapes

# broadcast the borough shapes to all nodes
borough_shapes_bc = sc.broadcast(borough_shapes)
point = Point(-73.98513, 40.758896)


def find_borough(lon, lat):
    if lon is None or lat is None:
        return None

    point = Point(lon, lat)

    for borough_name, borough_geometry in borough_shapes_bc.value.items():
        if borough_geometry.contains(point):
            return borough_name

    return "Unknown"


# Register the UDF
find_borough_udf = udf(find_borough, StringType())

# Step 4: Apply UDF to add pickup and dropoff borough information
enriched_df = df.withColumn(
    "pickup_borough", find_borough_udf(df["pickup_longitude"], df["pickup_latitude"])
).withColumn(
    "dropoff_borough", find_borough_udf(df["dropoff_longitude"], df["dropoff_latitude"])
)

# Display sample results
enriched_df.select(
    "hack_license",
    "pickup_longitude",
    "pickup_latitude",
    "pickup_borough",
    "dropoff_longitude",
    "dropoff_latitude",
    "dropoff_borough",
).show(5)
