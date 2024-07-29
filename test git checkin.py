# Databricks notebook source


# COMMAND ----------

storage_name = 'degroup1'
container_name = 'stock-data'
adls_raw_folder = f"abfss://stock-data@{storage_name}.dfs.core.windows.net"

# COMMAND ----------

df = spark.read.parquet(f'{adls_raw_folder}/overview')

# COMMAND ----------

display(df)
