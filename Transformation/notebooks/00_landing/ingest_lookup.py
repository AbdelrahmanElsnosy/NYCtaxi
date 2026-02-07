# Databricks notebook source
import urllib.request
import shutil
import os
url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

output_dir = "/Volumes/nyctaxi/00_landing/data_sources/lookup"

local_path = f"{output_dir}/taxi_zone_lookup.csv"

try :
    os.makedirs(output_dir, exist_ok=True)
    with urllib.request.urlopen(url) as response, open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "yes")
    print("File downloaded successfully!")
except Exception as e:
    print(f"Failed to download file: {e}") 
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "no")
    print("File downloaded failed")
