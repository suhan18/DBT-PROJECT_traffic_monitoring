# To run this file use the command below (python trial_spark.py doesn't work for me)
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 stream_spark.py

# Using the concept of Micro Batching from Big Data which gives the most accurate results
# This is used by spark by default for streaming data
# Micro batching is not exactly batch processing because what spark does it is it takes a batch of "NEW" data and processes it in a micro second

# How it works:
# Spark waits for data from Kafka for a micro second.
# All data received in that interval becomes 1 micro-batch.
# That micro-batch is processed just like a normal DataFrame.
# Result is output in almost real-time.


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as spark_sum, when, round, lit
from pyspark.sql.types import StructType, StringType, IntegerType

# This is just our Spark Session initialization
spark = SparkSession.builder \
    .appName("VehicleDataStreamingAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Schema for incoming Kafka data
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("vehicle", StringType()) \
    .add("count", IntegerType())


# Our Kafka topic names
getting_data_topic = "vehicle-data" # Topic name for incoming data
sending_data_topic = "traffic-analysis-output" # Topic name for outgoing data

# Function to send DataFrame to Kafka
def send_to_kafka(batch_df, batch_id):
    print(f"\n==== Batch ID: {batch_id} ====")
    batch_df.show(truncate=False)  # This prints the full DataFrame to terminal
    
    # Add batch_id column to DataFrame
    batch_with_id = batch_df.withColumn("batch_id", lit(batch_id))

    # Send to Kafka
    batch_with_id.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", sending_data_topic) \
        .save()

# Get streaming data from Kafka using the same topic as input producer
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", getting_data_topic) \
    .load()

# Parse value as JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# We want to find the count of each vehicle type
vehicle_counts = parsed_df.withColumn("bike_count", when(col("vehicle") == "bike", col("count")).otherwise(0)) \
    .withColumn("auto_count", when(col("vehicle") == "auto", col("count")).otherwise(0)) \
    .withColumn("car_count", when(col("vehicle") == "car", col("count")).otherwise(0)) \
    .withColumn("bus_count", when(col("vehicle") == "bus", col("count")).otherwise(0)) \
    .withColumn("truck_count", when(col("vehicle") == "truck", col("count")).otherwise(0))

# Also use when clause to classify the location into regions such as North, South, East, and West
region_df = vehicle_counts.withColumn(
    "region",
    when(col("location").isin("Hebbal", "Yelahanka", "Jakkur"), "North")
    .when(col("location").isin("Jayanagar", "Banashankari", "JP Nagar"), "South")
    .when(col("location").isin("Whitefield", "Marathahalli", "Indiranagar"), "East")
    .otherwise("West")
)

# Group by location & region only
agg_df = region_df.groupBy(
    col("location"),
    col("region")
).agg(
    spark_sum("bike_count").alias("Bikes_Presnt"),
    spark_sum("auto_count").alias("Autos_Present"),
    spark_sum("car_count").alias("Cars_Present"),
    spark_sum("bus_count").alias("Buses_Present"),
    spark_sum("truck_count").alias("Trucks_Present"),
).withColumn(
    "total_vehicle_count",
    col("Bikes_Presnt") + col("Autos_Present") + col("Cars_Present") + col("Buses_Present") + col("Trucks_Present")
).withColumn(
    "Traffic_Score",
    round(
        col("Bikes_Presnt") * 0.15 +
        col("Autos_Present") * 0.15 +
        col("Cars_Present") * 0.20 +
        col("Buses_Present") * 0.25 +
        col("Trucks_Present") * 0.25,
        2
    )
).withColumn(
    "Traffic_Level",
    when(col("Traffic_Score") < 10, "Low Traffic")
    .when((col("Traffic_Score") >= 10) & (col("Traffic_Score") <= 20), "Medium Traffic")
    .otherwise("High Traffic")
)


# Output to Console
query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(send_to_kafka) \
    .start()


query.awaitTermination()