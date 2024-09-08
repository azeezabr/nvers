# Databricks notebook source
businessDate = '2024-08-04'

# COMMAND ----------

import sys, os, importlib
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

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
sv_company_profile_nm = 'company_profile' 
sv_company_profile_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{sv_company_profile_nm}")
sv_company_profile_df = sv_company_profile_dt.toDF().filter(col('EndDate').isNull())

sv_company_metrics_nm = 'company_symbol_metrics' 
sv_company_metrics_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{sv_company_metrics_nm}")
sv_company_metrics_df = sv_company_metrics_dt.toDF().filter(col('EndDate') == businessDate)

gold_layer_path = f"{adls_path}/gold" 
gold_table_DimCompany_nm = 'DimCompany' 
gold_table_dt = DeltaTable.forPath(spark, f"{gold_layer_path}/{gold_table_DimCompany_nm}")
gold_table_df = gold_table_dt.toDF().filter(col('IsActive') == 'Y')



# COMMAND ----------

joined_df = sv_company_metrics_df.alias('sm') \
    .join(sv_company_profile_df.alias('cp'), 
          (col('sm.CompanyId') == col('cp.CompanyId')), 
          "inner")

# COMMAND ----------

joined_df = joined_df.withColumn(
    "CapCategory",
    when(col("MarketCapitalization") <= 100000000, "LOW")
    .when((col("MarketCapitalization") > 100000000) & (col("MarketCapitalization") <= 500000000), "MID")
    .otherwise("LARGE")
)


# COMMAND ----------

joined_df = joined_df.withColumn(
    "FloatCategory",
    when(col("SharesOutstanding") <= 1000000, "NANO")
    .when((col("SharesOutstanding") > 1000000) & (col("SharesOutstanding") <= 3000000), "BEAR LOW")
    .when((col("SharesOutstanding") > 3000000) & (col("SharesOutstanding") <= 5000000), "LOW")
    .when((col("SharesOutstanding") > 5000000) & (col("SharesOutstanding") <= 10000000), "DECENT")
    .when((col("SharesOutstanding") > 10000000) & (col("SharesOutstanding") <= 30000000), "ABOVE DECENT")
    .otherwise("BIGGER")
).withColumn("EffectiveFromDate", current_date()) \
     .withColumn("EndToDate", lit(None).cast(DateType())) \
     .withColumn("IsActive", lit("Y")) 

# COMMAND ----------

result_df = joined_df.select(
    col('EffectiveFromDate'),
    col('EndToDate'),
    col('cp.Symbol').alias('CompanySymbol'),
    col('cp.CompanyName'),
    col('cp.CompanyDescription'),
    col('cp.AssetType'),
    col('cp.Exchange'),
    col('cp.Currency'),
    col('cp.Country'),
    col('cp.Sector'),
    col('cp.Industry'),
    col('cp.Address'),
    col('cp.CompanyId'),
    col('CapCategory'),
    col('FloatCategory'),
    col('IsActive')
)


# COMMAND ----------

#silver_table_df.createTempView("silver_company_profile")

# COMMAND ----------


# Define merge condition
merge_condition = "target.CompanyId = source.CompanyId AND trim(target.IsActive) = 'Y'"

# Define update set and insert set dictionaries
update_set = {
    "EndToDate": "current_date()",
    "IsActive": "'N'"
}

insert_set = {
    "EffectiveFromDate": "source.EffectiveFromDate",
    "EndToDate": "source.EndToDate",
    "CompanySymbol": "source.CompanySymbol",
    "CompanyName": "source.CompanyName",
    "CompanyDescription": "source.CompanyDescription",
    "AssetType": "source.AssetType",
    "Exchange": "source.Exchange",
    "Currency": "source.Currency",
    "Country": "source.Country",
    "Sector": "source.Sector",
    "Industry": "source.Industry",
    "Address": "source.Address",
    "CompanyID": "source.CompanyId",
    "CapCategory": "source.CapCategory",
    "FloatCategory": "source.FloatCategory",
    "IsActive": "source.IsActive"
}

# Define update and insert conditions
update_condition = """
    trim(target.CompanyName) <> trim(source.CompanyName) OR
    trim(target.CompanyDescription) <> trim(source.CompanyDescription) OR
    trim(target.AssetType) <> trim(source.AssetType) OR
    trim(target.Exchange) <> trim(source.Exchange) OR
    trim(target.Currency) <> trim(source.Currency) OR
    trim(target.Country) <> trim(source.Country) OR
    trim(target.Sector) <> trim(source.Sector) OR
    trim(target.Industry) <> trim(source.Industry) OR
    trim(target.Address) <> trim(source.Address) OR
    trim(target.CapCategory) <> trim(source.CapCategory) OR
    trim(target.FloatCategory) <> trim(source.FloatCategory)
"""

insert_condition = "source.IsActive = 'N'"

 

# COMMAND ----------

gold_table_dt.alias("target").merge(
        result_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(condition= update_condition,
        set=update_set) \
     .execute()


# COMMAND ----------

gold_table_dt.alias("target").merge(
        result_df.alias("source"),
        merge_condition 
    ).whenNotMatchedInsert(
      values=insert_set) \
    .whenNotMatchedBySourceUpdate( \
        set=update_set \
    ).execute()

# COMMAND ----------

#display(result_df.filter(col('CompanySymbol') == "WING"))
#display(gold_table_dt.toDF().filter(col('CompanySymbol') == "WING"))



# COMMAND ----------

gold_table_dt.toDF().count()

# COMMAND ----------

'''
gold_table_dt.update(
    condition = expr("CompanySymbol = 'WING'"),
    set = {
        "CompanyName": expr("'test test'")
    }
)
'''

# COMMAND ----------

#gold_table_dt.delete()

# COMMAND ----------


