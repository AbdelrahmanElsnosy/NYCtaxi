# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df_trips = spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed")
df_trips.display()

# COMMAND ----------

df_zones = spark.read.table("nyctaxi.02_silver.taxi_zone_lookup")
df_zones.display()

# COMMAND ----------

df_result = (
    df_trips.alias("t")
    .join(df_zones.alias("pu"),
          col("t.pickup_location_id") == col("pu.location_id"),
          "left")
    .join(df_zones.alias("do"),
          col("t.dropoff_location_id") == col("do.location_id"),
          "left")
    .select(
        col("t.vendor"),
        col("t.tpep_pickup_datetime"),
        col("t.tpep_dropoff_datetime"),
        col("t.trip_duration"),
        col("t.passenger_count"),
        col("t.trip_distance"),
        col("t.rate_type"),
        col("pu.borough").alias("pu_borough"),
        col("pu.zone").alias("pu_zone"),
        col("do.borough").alias("do_borough"),
        col("do.zone").alias("do_zone"),
        col("t.payment_type"),
        col("t.fare_amount"),
        col("t.extra"),
        col("t.mta_tax"),
        col("t.tip_amount"),
        col("t.tolls_amount"),
        col("t.improvement_surcharge"),
        col("t.total_amount"),
        col("t.congestion_surcharge"),
        col("t.airport_fee"),
        col("t.cbd_congestion_fee"),
        col("t.processd_time")
    )
)

df_result.display()

# COMMAND ----------

df_result.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_enriched")