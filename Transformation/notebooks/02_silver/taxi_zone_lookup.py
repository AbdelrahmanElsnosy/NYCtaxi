# Databricks notebook source
from pyspark.sql.functions import lower, col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, TimestampType
from datetime import datetime
from delta.tables import DeltaTable


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

end_date = datetime.now()
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

# check if there are any updated values for all active records
dt.alias("t").merge(
    df.alias("s"),
    "t.location_id = s.location_id AND t.end_date IS NULL AND (s.borough != t.borough OR s.zone != t.zone OR s.service_zone != t.service_zone)"   
    ).whenMatchedUpdate(set={"end_date": lit(end_date).cast(TimestampType())}).execute()

# COMMAND ----------

# insert updated records
list_new_records = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_date}'").select("location_id").collect()]
# insert new records
if len(list_new_records) == 0:
    print("No new records to insert")
else:
    dt.alias("t").merge(
        source = df.alias("s"),
        condition = f"s.location_id NOT IN ({','.join(map(str,list_new_records))})"
        ).whenNotMatchedInsert(values = {
            "location_id": col("s.location_id"),
            "borough": col("s.borough"),
            "zone": col("s.zone"),
            "service_zone": col("s.service_zone"),
            "ingestion_ts": current_timestamp(),
            "end_date": lit(None).cast(TimestampType())
            }).execute()

# COMMAND ----------

# insert records for completely new ids
dt.alias("t").merge(
    source = df.alias("s"),
    condition = "t.location_id = s.location_id",
    ).whenNotMatchedInsert(values = {
        "location_id": col("s.location_id"),
        "borough": col("s.borough"),
        "zone": col("s.zone"),
        "service_zone": col("s.service_zone"),
        "ingestion_ts": current_timestamp(),
        "end_date": lit(None).cast(TimestampType())
        }).execute()