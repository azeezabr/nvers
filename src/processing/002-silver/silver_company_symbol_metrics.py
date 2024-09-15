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

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
symbol_mapping_table_path = f"{silver_layer_path}/mapping/symbol_mapping"
silver_table_name = 'company_symbol_metrics'
bronze_table_name = f'overview/year={year}/month={month}/day={day}'

# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")
symbol_mapping_df = util.read_delta_to_df(spark, f"{symbol_mapping_table_path}")

# COMMAND ----------

bronze_df = util.load_bronze_data(spark,bronze_layer_path,bronze_table_name )
#bronze_df = bronze_df.filter(bronze_df.Symbol == 'MSFT')
#display(bronze_df.count())

# COMMAND ----------

company_profile_df = bronze_df.join(symbol_mapping_df, on="Symbol", how="left").select(
        "CompanyId",
        "Symbol",
        "MarketCapitalization" ,
        "SharesOutstanding" ,
        "52WeekHigh" ,
        "52WeekLow" ,
        "50DayMovingAverage" ,
        "200DayMovingAverage" ,
        "ReturnOnEquityTTM" ,
        "DividendPerShare" ,
        "DividendYield" ,
        "DividendDate" ,
        "ExDividendDate" 
    ).withColumn("EffectiveDate", current_date()) \
     .withColumn("EndDate", current_date()) \
     .withColumn("IsCurrent", lit("Y")) 


# COMMAND ----------

merge_condition  = "source.IsCurrent = 'N'"
    
insert_set = {
        "CompanyId": "source.CompanyId",
        "Symbol": "source.Symbol",
        "MarketCapitalization": "source.MarketCapitalization",
        "SharesOutstanding": "source.SharesOutstanding",
        "52WeekHigh": "source.52WeekHigh",
        "52WeekLow": "source.52WeekLow",
        "50DayMovingAverage": "source.50DayMovingAverage",
        "200DayMovingAverage": "source.200DayMovingAverage",
        "ReturnOnEquityTTM": "source.ReturnOnEquityTTM",
        "DividendPerShare": "source.DividendPerShare",
        "DividendYield": "source.DividendYield",
        "DividendDate": "source.DividendDate",
        "ExDividendDate": "source.ExDividendDate",
        "EffectiveDate": "source.EffectiveDate",
        "EndDate": "source.EndDate",
        "IsCurrent": "source.IsCurrent",
    }



# COMMAND ----------

silver_table_dt.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition 
    ).whenNotMatchedInsert( \
      values=insert_set).execute()

# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")
silver_customer_profile_df = silver_table_dt.toDF()
#display(silver_customer_profile_df)


# COMMAND ----------

#silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")

#silver_table_dt.delete()

# COMMAND ----------


