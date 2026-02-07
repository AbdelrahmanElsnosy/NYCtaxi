# Databricks notebook source
from pyspark.sql.functions import lower, col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, TimestampType


# COMMAND ----------

csv_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(csv_path)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").cast(StringType()).alias("borough"),
    col("Zone").cast(StringType()).alias("zone"),
    col("service_zone").cast(StringType()).alias("service_zone"),
    current_timestamp().alias("ingestion_ts"),
    lit(None).cast(TimestampType()).alias("end_date")
    )

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")