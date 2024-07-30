# Databricks notebook source
import sys
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

path = '/nvers/src/common/'
#/Workspace/Users/azbms006@gmail.com/nvers/src/common/schemas/overview.py

if path not in sys.path:
    sys.path.append(path)
import schemas.silver.company_profile as sv


# COMMAND ----------

schem = sv.company_profile_schema()
print(schem)

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_raw_folder = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_table_path = f"{adls_raw_folder}/bronze"

# COMMAND ----------

def load_bronze_data(base,table_name):
    return spark.read.format("parquet").load(f'{base}/{bronze_table_path}')

# COMMAND ----------

df = load_bronze_data()

# COMMAND ----------

d#f = spark.read.parquet(f'{adls_raw_folder}/overview')

# COMMAND ----------

display(df)

# COMMAND ----------


