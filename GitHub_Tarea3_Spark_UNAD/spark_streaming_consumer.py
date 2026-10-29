#!/usr/bin/env python3
"""Lee sensor_data de Kafka y calcula medias por ventana de un minuto."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

spark = (SparkSession.builder
    .appName("SensorDataAnalysis")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType()),
])

df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .load())

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

windowed_stats = (parsed_df
    .groupBy(window(col("event_time"), "1 minute"), col("sensor_id"))
    .agg(avg("temperature").alias("temperatura_promedio"),
         avg("humidity").alias("humedad_promedio"),
         count("*").alias("total_lecturas")))

query = (windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start())
query.awaitTermination()
