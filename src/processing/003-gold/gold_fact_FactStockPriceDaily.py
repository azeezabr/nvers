# Databricks notebook source
dbutils.widgets.text("year", "")
dbutils.widgets.text("month", "")
dbutils.widgets.text("day", "")


year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")
day = dbutils.widgets.get("day")

# COMMAND ----------

import sys, os, importlib
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col, when, expr, to_date, unix_timestamp, from_unixtime, current_timestamp, date_format, to_timestamp, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

#businessDate = to_date(lit(businessDate), 'yyyy-MM-dd') 

# COMMAND ----------

paths_and_modules = {
    
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.company_profile'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/': ['utils'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.util_func']
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
 
 



# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"


silver_layer_path = f"{adls_path}/silver" 
sv_stock_price_daily_nm = 'stock_price_daily' 
sv_stock_price_daily_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{sv_stock_price_daily_nm}/year={year}/month={month}/day={day}")
sv_stock_price_daily_df = sv_stock_price_daily_dt.toDF()


 
gold_layer_path = f"{adls_path}/gold" 
gold_table_DimCompany_nm = 'DimCompany' 
gold_table_DimCompany_dt = DeltaTable.forPath(spark, f"{gold_layer_path}/{gold_table_DimCompany_nm}")
gold_table_DimCompany_df = gold_table_DimCompany_dt.toDF().filter(col('IsActive') == 'Y')

 
gold_table_FactStorckPriceDaily_nm = 'FactStockPriceDaily' 
 


# COMMAND ----------

#display(gold_table_DimCompany_df)

# COMMAND ----------

#spark.sql(f"SHOW PARTITIONS delta.`{silver_layer_path}/{sv_stock_price_daily_nm}`").show()

# COMMAND ----------

notebook_path = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath())


result_df = sv_stock_price_daily_df \
    .withColumn("GapUpPerc", round((col('Close') - col('Open')) / col('Close') * 100, 2).cast("double")) \
    .withColumn("DimDateKey", date_format(to_date(col("TradeDate"), 'yyyy-MM-dd'), "yyyyMMdd").cast("int")) \
    .withColumn("ProcessDate", current_timestamp()) \
    .withColumn("JobName", lit(notebook_path))
    

# COMMAND ----------

#display(result_df)

# COMMAND ----------

join_condition = result_df["CompanyId"] == gold_table_DimCompany_df["CompanyId"]

final_df = result_df.join(
    gold_table_DimCompany_df,
    join_condition,
    "left"
).select(
    result_df["*"],  
    gold_table_DimCompany_df["DimCompanyID"]  
)

# COMMAND ----------

#display(gold_table_DimCompany_df)

# COMMAND ----------

final_df = final_df.withColumn("DimCompanyKey", col("DimCompanyID"))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#final_df.printSchema()
#gold_table_FactCompany_dt.toDF().printSchema()

# COMMAND ----------

final_df = final_df.drop("StockPriceId", "CompanyId", "Symbol","TradeDate","DimCompanyID","EffectiveDate","year","month","day")

# COMMAND ----------

final_df = final_df.coalesce(4)

# COMMAND ----------

#print(final_df.rdd.getNumPartitions())

# COMMAND ----------

#display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("append").save(f'{gold_layer_path}/{gold_table_FactStorckPriceDaily_nm}')


# COMMAND ----------

#gold_table_FactCompany_dt = DeltaTable.forPath(spark, f"{gold_layer_path}/{gold_table_FactStorckPriceDaily_nm}")
#gold_table_FactCompany_df = gold_table_FactCompany_dt.toDF()
#display(gold_table_FactCompany_dt.toDF().filter(col('DimCompanyKey').isNull()))
#gold_table_FactCompany_dt.delete()

# COMMAND ----------

#gold_table_FactCompany_dt.delete()

# COMMAND ----------


