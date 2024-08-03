# Databricks notebook source
import sys, os
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

usr_path = dbutils.secrets.get(scope="nvers", key="usr_dir")
path = f'{usr_path}/nvers/src/common/schemas/'

if path not in sys.path:
    #sys.path.append(path)
    sys.path.append(os.path.abspath(path))

import silver.company_profile as sv
importlib.reload(sv)


# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"
bronze_layer_path = f"{adls_path}/bronze"
silver_layer_path = f"{adls_path}/silver"
mapping_table_path = f"{adls_path}/bronze/mapping"
silver_table_name = 'company_profile'
bronze_table_name = 'overview'

# COMMAND ----------

usr_path = dbutils.secrets.get(scope="nvers", key="usr_dir")
path = f'{usr_path}/nvers/src/common/'

if path not in sys.path:
    #sys.path.append(path)
    sys.path.append(os.path.abspath(path))


import utils as sv
importlib.reload(sv)

#sv.market_hours_generator()

#bronze_df = sv.load_bronze_data(bronze_layer_path,bronze_table_name )


# COMMAND ----------

schem = sv.company_profile_schema()
print(schem)

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
mapping_table_path = f"{adls_path}/silver/mapping"
table = 'company_profile'

# COMMAND ----------

def load_bronze_data(base,table_name):
    return spark.read.parquet(f'{base}/{table_name}')

# COMMAND ----------

table_name = 'overview'
bronze_df = load_bronze_data(bronze_layer_path,table_name )
bronze_df = bronze_df.filter(bronze_df.Symbol == 'MSFT')

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

mapping_df.count()

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

company_profile_df.count()

# COMMAND ----------


def upsert_company_profile(company_profile_df, company_profile_table_path):
    company_profile_table = DeltaTable.forPath(spark, company_profile_table_path)
    
    merge_condition = """
    target.CompanyId = source.CompanyId AND target.IsCurrent = 'Y'
    
    AND (
        trim(target.CompanyName) <> trim(source.CompanyName) OR
        trim(target.CompanyDescription) <> trim(source.CompanyDescription) OR
        trim(target.AssetType) <> trim(source.AssetType) OR
        trim(target.Exchange) <> trim(source.Exchange) OR
        trim(target.Currency) <> trim(source.Currency) OR
        trim(target.Country) <> trim(source.Country) OR
        trim(target.Sector) <> trim(source.Sector) OR
        trim(target.Industry) <> trim(source.Industry) OR
        trim(target.Address) <> trim(source.Address)
        )"""
    
    update_set = {
        "EndDate": "source.EffectiveDate",
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
    
    company_profile_table.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(set=update_set) \
     .whenNotMatchedInsert(values=insert_set) \
     .execute()

# COMMAND ----------

'''delta_table_path = f"{silver_layer_path}/{table}"

 
spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        CompanyId LONG,
        Symbol STRING,
        CompanyName STRING,
        CompanyDescription STRING,
        AssetType STRING,
        Exchange STRING,
        Currency STRING,
        Country STRING,
        Sector STRING,
        Industry STRING,
        Address STRING,
        EffectiveDate DATE,
        EndDate DATE,
        IsCurrent STRING,
        CurrentLoadFlg STRING
    ) USING DELTA
""")
'''


# COMMAND ----------


company_profile_table = DeltaTable.forPath(spark, f"{silver_layer_path}/{table}")

    
merge_condition = """
    trim(target.CompanyId) = trim(source.CompanyId) AND trim(target.IsCurrent) = 'Y'

    """
    
    
update_set = {
        "EndDate": "source.EffectiveDate",
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
    
    

# COMMAND ----------

comp_df = company_profile_table.toDF()
comp_df.count()
display(comp_df)
display(company_profile_df)

# COMMAND ----------

company_profile_table.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(condition="""
                        
        trim(target.CompanyName) <> trim(source.CompanyName) OR
        trim(target.CompanyDescription) <> trim(source.CompanyDescription) OR
        trim(target.AssetType) <> trim(source.AssetType) OR
        trim(target.Exchange) <> trim(source.Exchange) OR
        trim(target.Currency) <> trim(source.Currency) OR
        trim(target.Country) <> trim(source.Country) OR
        trim(target.Sector) <> trim(source.Sector) OR
        trim(target.Industry) <> trim(source.Industry) OR
        trim(target.Address) <> trim(source.Address)
        """,
        set=update_set) \
     .execute()

     #condition = "source.IsCurrent = 'Y'",

# COMMAND ----------

merge_condition = """
    source.IsCurrent = 'N'
    """



company_profile_table.alias("target").merge(
        company_profile_df.alias("source"),
        merge_condition 
    ).whenNotMatchedInsert(
    values=insert_set) \
     .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC '''OR
# MAGIC     (target.CompanyId <> source.CompanyId)
# MAGIC     AND target.CurrentLoadFlg = 'Y'
# MAGIC     '''

# COMMAND ----------

company_profile_df = company_profile_df.withColumn("CompanyDescription", lit('SDSDS'))

# COMMAND ----------

company_delta_table = DeltaTable.forPath(spark, company_delta)

company_delta_table.delete()

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

# COMMAND ----------

#company_delta_table = DeltaTable.forPath(spark, company_delta)

company_delta_table = spark.read.format("delta").load(company_delta)
company_delta_table.createOrReplaceTempView("company_delta")
count_query = "SELECT COUNT(*) AS count FROM company_delta"
count_result = spark.sql(count_query).first()["count"]
print(count_result)

# COMMAND ----------

df = spark.read.format("delta").load(company_delta)
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

#display(people_df.filter("Symbol = 'CME'"))

# COMMAND ----------

#display(bronze_df.filter("symbol = 'CLMB'"))

# COMMAND ----------

#company_profile_table.delete(col("Symbol") == "CME")

# COMMAND ----------

#mapping_df = spark.read.format("delta").load(mapping_table_path)

# COMMAND ----------

#mapping_df.count()

# COMMAND ----------



# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
