# Databricks notebook source
from pyspark.sql.functions import max, min, sum, avg, col, concat, lit

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")
df.display()

# COMMAND ----------

df_max_vendor_revenue = (
    df.groupBy("vendor")
      .agg(sum("total_amount").alias("sum_revenue"))
      .orderBy(col("sum_revenue").desc())
)
df_max_vendor_revenue.display()

# COMMAND ----------

df_most_popular_pickup_borough = df.groupBy("pu_borough").count().orderBy(col("count").desc())
df_most_popular_pickup_borough.display()

# COMMAND ----------

df_most_common_journey = df.groupBy(concat("pu_borough", lit("->"), "do_borough")).count().orderBy(col("count").desc())
df_most_common_journey.display()

# COMMAND ----------

df2 = spark.read.table("nyctaxi.03_gold.gold_daily_trips_summary")
df2.display()