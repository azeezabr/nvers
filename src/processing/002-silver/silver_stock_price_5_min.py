# Databricks notebook source
import sys, os, importlib
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")
dbutils.widgets.text("day", "")


year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
day = dbutils.widgets.get("day")

# COMMAND ----------

usr_path = dbutils.secrets.get(scope="nvers", key="usr_dir")

paths_and_modules = {
    f'{usr_path}/nvers/src/common/schemas/': ['silver.company_profile'],
    f'{usr_path}/nvers/src/common/': ['utils'],
    f'{usr_path}/nvers/src/common/schemas/': ['silver.util_func'],
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

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
symbol_mapping_table_path = f"{silver_layer_path}/mapping/symbol_mapping"
silver_table_name = 'stock_price_5_min'
bronze_table_name = 'stock-data-intraday/timeseries-5m/year=2024/month=3/'

# COMMAND ----------

spark._jsc.hadoopConfiguration().set(f"fs.azure.account.key.{storage_name}.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")
symbol_mapping_df = util.read_delta_to_df(spark, f"{symbol_mapping_table_path}")

# COMMAND ----------

bronze_df = util.load_bronze_data(spark,bronze_layer_path,bronze_table_name )
#bronze_df = bronze_df.filter(bronze_df.symbol == 'MSFT')
#display(bronze_df)
bronze_df = bronze_df.selectExpr("symbol as Symbol", "cast(timestamp as Timestamp) as TradeDate", "CAST(open as DOUBLE) as Open", "CAST(high as DOUBLE) as High", "CAST(low as DOUBLE) as Low", "CAST(close as DOUBLE) as Close", "CAST(volume as BIGINT) as Volume", " cast(date_format(timestamp, 'yyyyMM') as INT) as TradeYearMonth", "CAST(hour as INT) as Hour")  
#display(bronze_df  )
 

# COMMAND ----------

company_profile_df = bronze_df.join(symbol_mapping_df, on="Symbol", how="left").select(
        "CompanyId",
        "Symbol",
        "TradeDate" ,
        "Open" ,
        "High" ,
        "Low" ,
        "Close" ,
        "Volume" ,
        "TradeYearMonth",
        "Hour" ,
    ).withColumn("EffectiveDate", current_date()) 
 


# COMMAND ----------

#display(company_profile_df)

# COMMAND ----------

company_profile_df.write.format("delta").mode("append").save(f'{silver_layer_path}/{silver_table_name}')


# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")
silver_customer_profile_df = silver_table_dt.toDF()
#display(silver_customer_profile_df)
#print(silver_customer_profile_df.rdd.getNumPartitions())
#silver_customer_profile_df.count()

# COMMAND ----------

#silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")

#silver_table_dt.delete()
