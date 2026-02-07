# Databricks notebook source
import urllib.request
import shutil
import os
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta
two_months_ago = date.today() - relativedelta(months=2)
two_months_ago_formated = two_months_ago.strftime("%Y-%m")
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{two_months_ago_formated}"
local_path = f"{dir_path}/yellow_trips_{two_months_ago_formated}.parquet"
try:
    dbutils.fs.ls(local_path)
    dbutils.jobs.taskValues.set(key = "continue_downstream", value = "no")
    print("File already downloaded skipping the down stream")

except:
      try:
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        url = f"{base_url}/yellow_tripdata_{two_months_ago_formated}.parquet"
        print(f"Downloading: {url}")

        output_dir = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{two_months_ago_formated}"
        os.makedirs(output_dir, exist_ok=True)

        file_path = f"{output_dir}/yellow_tripdata_{two_months_ago_formated}.parquet"

        with urllib.request.urlopen(url) as response, open(file_path, 'wb') as f:
            shutil.copyfileobj(response, f)
        dbutils.jobs.taskValues.set(key = "continue_downstream", value = "yes")
        print(f"Saved to: {file_path}\n")
      except Exception as e:
          dbutils.jobs.taskValues.set(key = "continue_downstream", value = "no")
          print(f"Failed for {two_months_ago_formated}: {e}\n")
