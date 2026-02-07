# Databricks notebook source
from pyspark.sql.functions import count, min, max, avg, sum, round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df.display()

# COMMAND ----------

df_result = df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(
        count("*").alias("total_trips"), 
        round(avg("passenger_count"), 1).alias("avg_passenger_count"),
        round(avg("trip_distance"), 1).alias("avg_trip_distance"),
        round(avg("fare_amount"), 1).alias("avg_fare_amount"),
        max("fare_amount").alias("max_fare_amount"),
        min("fare_amount").alias("min_fare_amount"),
        round(sum("total_amount"), 1).alias("total_revenue")
        )
df_result.display()

# COMMAND ----------

df_result.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.gold_daily_trips_summary")