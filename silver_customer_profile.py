# Databricks notebook source
import sys
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

path = '/Workspace/Users/azbms006@gmail.com/nvers/src/common/'
#/Workspace/Users/azbms006@gmail.com/nvers/src/common/schemas/overview.py

if path not in sys.path:
    sys.path.append(path)
import schemas.silver as sv


# COMMAND ----------

#schem = sv.company_profile.company_profile_schema()
#print(schem)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, 
    StructField, 
    IntegerType,
    StringType,
    DateType,
    
)

def company_profile_schema(): 
    return StructType([
        StructField("CompanyId", IntegerType(), False), 
        StructField("Symbol", StringType(), True), 
        StructField("CompanyName", StringType(), True),
        StructField("CompanyDescription", StringType(), True),
        StructField("AssetType", StringType(), True),
        StructField("Exchange", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Sector", StringType(), True),
        StructField("Industry", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("EffectiveDate", DateType(), True),
        StructField("EndDate", DateType(), True),
        StructField("IsCurrent", StringType(), True)
    ])

def symbol_mapping_schema():
    return StructType([
        StructField("CompanyId", StringType(), False), 
        StructField("Symbol", IntegerType(), False),
        StructField("EffectiveDate", DateType(), True), 
    ])

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
mapping_table_path = f"{adls_path}/bronze/mapping"

# COMMAND ----------

def load_bronze_data(base,table_name):
    return spark.read.parquet(f'{base}/{table_name}')

# COMMAND ----------

table_name = 'overview'
bronze_df = load_bronze_data(bronze_layer_path,table_name )

# COMMAND ----------

def update_symbol_mapping(bronze_df, mapping_table_path):
    try:
        mapping_df = spark.read.format("delta").load(mapping_table_path)
    except:
        mapping_df = spark.createDataFrame([], symbol_mapping_schema())
    new_symbols_df = bronze_df.select("Symbol").distinct().subtract(mapping_df.select("Symbol").distinct())
    

    if new_symbols_df.count() > 0:
        new_mapping_df = new_symbols_df.withColumn("CompanyId", monotonically_increasing_id() + mapping_df.count())\
            .withColumn("EffectiveDate", current_date())

        updated_mapping_df = mapping_df.union(new_mapping_df)
        
        updated_mapping_df.write.format("delta").mode("overwrite").save(mapping_table_path)
    else:
        updated_mapping_df = mapping_df
    
    return updated_mapping_df

# COMMAND ----------

mapping_df = update_symbol_mapping(bronze_df, mapping_table_path)

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


def upsert_company_profile(company_profile_df, company_profile_table_path):
    company_profile_table = DeltaTable.forPath(spark, company_profile_table_path)
    
    merge_condition = "target.Symbol = source.Symbol AND target.IsCurrent = 'Y'"
    
    update_set = {
        "EndDate": "source.EffectiveDate",
        "IsCurrent": "'N'"
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
        "IsCurrent": "source.IsCurrent"
    }
    
    company_profile_table.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(set=update_set) \
     .whenNotMatchedInsert(values=insert_set) \
     .execute()

# COMMAND ----------

table = 'company_profile'
upsert_company_profile(company_profile_df,f"{silver_layer_path}/{table}")

# COMMAND ----------

'''
from delta.tables import DeltaTable
import os

def initial_write_company_profile(company_profile_df, company_profile_table_path):
    # Check if the Delta table already exists
    if not DeltaTable.isDeltaTable(spark, company_profile_table_path):
        # If the table does not exist, write the DataFrame to Delta table
        company_profile_df.write.format("delta").mode("overwrite").save(company_profile_table_path)
    else:
        print("Delta table already exists. Skipping initial write.")

initial_write_company_profile(company_profile_df,f"{silver_layer_path}/{table}")
'''

# COMMAND ----------

company_profile_table = DeltaTable.forPath(spark, f"{silver_layer_path}/{table}")
company_delta = f"{silver_layer_path}/{table}"
print(company_delta)

# COMMAND ----------

company_delta_table = DeltaTable.forPath(spark, company_delta)

# COMMAND ----------


people_df = spark.read.format("delta").load(company_delta)
display(people_df)


# COMMAND ----------

#company_delta_table.delete()

# COMMAND ----------


