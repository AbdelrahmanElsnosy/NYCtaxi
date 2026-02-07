# Databricks notebook source
from pyspark.sql.functions import timestamp_diff, col, when, min, max
from dateutil.relativedelta import relativedelta
from datetime import datetime, date

# COMMAND ----------

two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)
one_month_ago_start = date.today().replace(day=1) - relativedelta(months=1)

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").filter(f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{one_month_ago_start}'")

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime <= '2025-12-31'")

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1 , "Creative Mobile Technologies, LLC")
    .when(col("VendorID") == 2 , "Curb Mobility, LLC")
    .when(col("VendorID") == 6, "Myle Technologies Inc")
    .when(col("VendorID") == 7, "Helix")
    .otherwise("Unknown")
    .alias("vendor"),

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    timestamp_diff('MINUTE',col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")).alias("trip_duration"),

    "passenger_count",
    "trip_distance",

    when(col("ratecodeid") == 1, "Standard rate")
    .when(col("ratecodeid") == 2, "JFK")
    .when(col("ratecodeid") == 3, "Newark")
    .when(col("ratecodeid") == 4, "Nassau or Westchester")
    .when(col("ratecodeid") == 5, "Negotiated fare")
    .when(col("ratecodeid") == 6, "Group ride")
    .otherwise("Unknown")
    .alias("rate_type"),

    "store_and_fwd_flag",
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),

    when(col("payment_type") == 1, "Credit card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided trip")
    .otherwise("Unknown")
    .alias("payment_type"),

    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "processd_time"

)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.`02_silver`.yellow_trips_cleansed")