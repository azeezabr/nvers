# Databricks notebook source
import sys, os, importlib
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

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

#utils.market_hours_generator()
#bronze_df = utils.load_bronze_data(bronze_layer_path,bronze_table_name )

#bronze_df = ut.load_bronze_data(spark,bronze_layer_path,bronze_table_name )

 



# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
mapping_table_path = f"{silver_layer_path}/mapping/symbol_mapping"
silver_table_name = 'company_profile'
bronze_table_name = 'overview'
silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")

# COMMAND ----------

bronze_df = util.load_bronze_data(spark,bronze_layer_path,bronze_table_name )
#bronze_df = bronze_df.filter(bronze_df.Symbol == 'MSFT')
#display(bronze_df)

# COMMAND ----------

mapping_schema = sv.symbol_mapping_schema()
mapping_df = util.update_symbol_mapping(spark,bronze_df, mapping_table_path,mapping_schema)

# COMMAND ----------

display(mapping_df)

# COMMAND ----------

company_profile_df = bronze_df.join(mapping_df, on="Symbol", how="inner").select(
        "CompanyId",
        "Symbol",
        "Name",
        "Description",
        "AssetType",
        "Exchange",
        "Currency",
        "Country",
        "Sector",
        "Industry",
        "Address"
    ).withColumnRenamed("Name", "CompanyName") \
     .withColumnRenamed("Description", "CompanyDescription") \
     .withColumn("EffectiveDate", current_date()) \
     .withColumn("EndDate", lit(None).cast(DateType())) \
     .withColumn("IsCurrent", lit("Y")) 


# COMMAND ----------

merge_condition = "trim(target.CompanyId) = trim(source.CompanyId) AND trim(target.IsCurrent) = 'Y'"
    
    
update_set = {
        "EndDate": current_date(),
        "IsCurrent": "'N' "
    }
    
insert_set = {
        "CompanyId": "source.CompanyId",
        "Symbol": "source.Symbol",
        "CompanyName": "source.CompanyName",
        "CompanyDescription": "source.CompanyDescription",
        "AssetType": "source.AssetType",
        "Exchange": "source.Exchange",
        "Currency": "source.Currency",
        "Country": "source.Country",
        "Sector": "source.Sector",
        "Industry": "source.Industry",
        "Address": "source.Address",
        "EffectiveDate": "source.EffectiveDate",
        "EndDate": "source.EndDate",
        "IsCurrent": "source.IsCurrent",
    }
    
update_condition = """
                        
        trim(target.CompanyName) <> trim(source.CompanyName) OR
        trim(target.CompanyDescription) <> trim(source.CompanyDescription) OR
        trim(target.AssetType) <> trim(source.AssetType) OR
        trim(target.Exchange) <> trim(source.Exchange) OR
        trim(target.Currency) <> trim(source.Currency) OR
        trim(target.Country) <> trim(source.Country) OR
        trim(target.Sector) <> trim(source.Sector) OR
        trim(target.Industry) <> trim(source.Industry) OR
        trim(target.Address) <> trim(source.Address)
        """

insert_condition = merge_condition = "source.IsCurrent = 'N'"


# COMMAND ----------

silver_table_dt.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(condition= update_condition,
        set=update_set) \
     .execute()


# COMMAND ----------

silver_table_dt.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition 
    ).whenNotMatchedInsert(
      values=insert_set) \
    .whenNotMatchedBySourceUpdate( \
        set=update_set \
    ).execute()

# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")
silver_customer_profile_df = silver_table_dt.toDF()
display(silver_customer_profile_df)



# COMMAND ----------

silver_table_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{silver_table_name}")

#silver_table_dt.delete()

# COMMAND ----------

display(silver_table_dt.toDF())

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
