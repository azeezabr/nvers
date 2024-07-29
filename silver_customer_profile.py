# Databricks notebook source
import sys

path = 'nvers/src/common/'

if path not in sys.path:
    sys.path.append(path)

import schemas.overview as ov



# COMMAND ----------



# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_raw_folder = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"

# COMMAND ----------

df = spark.read.parquet(f'{adls_raw_folder}/overview')

# COMMAND ----------

display(df)

# COMMAND ----------


