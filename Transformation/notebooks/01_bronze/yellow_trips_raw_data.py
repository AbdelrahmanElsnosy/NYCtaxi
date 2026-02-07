# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from datetime import datetime,timezone, date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_months_ago = date.today() - relativedelta(months=2)
two_months_ago_formatted = two_months_ago.strftime("%Y-%m")
print(two_months_ago_formatted)

# COMMAND ----------

df = spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{two_months_ago_formatted}")

# COMMAND ----------

df = df.withColumn("processd_time", current_timestamp())

# COMMAND ----------

df.write.mode("append").saveAsTable("nyctaxi.`01_bronze`.yellow_trips_raw")