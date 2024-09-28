# Databricks notebook source
import sys, os, importlib
import importlib
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")
dbutils.widgets.text("day", "")


year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
day = dbutils.widgets.get("day")

# COMMAND ----------

paths_and_modules = {
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.company_profile'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/': ['utils'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.util_func'],
}

for path, modules in paths_and_modules.items():
    abs_path = os.path.abspath(path)
    if abs_path not in sys.path:
        sys.path.append(abs_path)
    
    for module in modules:
        globals()[module] = importlib.import_module(module)
        importlib.reload(globals()[module])

import silver.company_profile as sv
import utils
import silver.util_func as util

schem = sv.company_profile_schema()



# COMMAND ----------

def symbol_mapping_schema():
    return StructType([
        StructField("Symbol", StringType(), False), 
        StructField("CompanyId", LongType(), False),
        StructField("EffectiveDate", DateType(), True), 
    ])

# COMMAND ----------

spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{dbutils.secrets.get('nvers','storage_name')}.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
mapping_table_path = f"{silver_layer_path}/mapping/symbol_mapping"
bronze_table_name = f'stock-list/year={year}/month={month}/day={day}'

# COMMAND ----------

bronze_df = util.load_bronze_data(spark,bronze_layer_path,bronze_table_name,'csv')

# COMMAND ----------

bronze_df = bronze_df \
            .filter(F.col("assetType") == 'Stock') \
            .withColumnRenamed("symbol","Symbol") \
            .withColumn("EffectiveDate", F.current_date()) \
            .select("Symbol","EffectiveDate")

# COMMAND ----------

mapping_schema = symbol_mapping_schema()
#print(mapping_schema)


# COMMAND ----------

#mapping_df = util.read_delta_to_df(spark,mapping_table_path)
#mapping_df.count()

# COMMAND ----------

mapping_df = util.update_symbol_mapping(spark,bronze_df, mapping_table_path,mapping_schema)

# COMMAND ----------


