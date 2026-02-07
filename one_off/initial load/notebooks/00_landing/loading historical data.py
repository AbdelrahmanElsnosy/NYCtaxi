# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

dates_to_process = [
    '2025-05','2025-06','2025-07','2025-08',
    '2025-09','2025-10','2025-11'
]

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for date in dates_to_process:
    try:
        url = f"{base_url}/yellow_tripdata_{date}.parquet"
        print(f"Downloading: {url}")

        output_dir = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{date}"
        os.makedirs(output_dir, exist_ok=True)

        file_path = f"{output_dir}/yellow_tripdata_{date}.parquet"

        with urllib.request.urlopen(url) as response, open(file_path, 'wb') as f:
            shutil.copyfileobj(response, f)

        print(f"Saved to: {file_path}\n")

    except Exception as e:
        print(f"Failed for {date}: {e}\n")

# COMMAND ----------

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

output_dir = "/Volumes/nyctaxi/00_landing/data_sources/lookup"

local_path = f"{output_dir}/taxi_zone_lookup.csv"

try :
    os.makedirs(output_dir, exist_ok=True)
    with urllib.request.urlopen(url) as response, open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
except Exception as e:
    print(f"Failed to download file: {e}")  