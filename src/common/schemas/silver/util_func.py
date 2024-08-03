import sys, os
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

def load_bronze_data(spark,base,table_name):
    return spark.read.parquet(f'{base}/{table_name}')



def update_symbol_mapping(spark,bronze_df, mapping_table_path,mapping_schema):
    try:
        mapping_df = spark.read.format("delta").load(mapping_table_path)
    except:
        mapping_df = spark.createDataFrame([], mapping_schema)
    new_symbols_df = bronze_df.select("Symbol").distinct().subtract(mapping_df.select("Symbol").distinct())
    

    if new_symbols_df.count() > 0:
        new_mapping_df = new_symbols_df.withColumn("CompanyId", monotonically_increasing_id() + mapping_df.count())\
            .withColumn("EffectiveDate", current_date())

        updated_mapping_df = mapping_df.union(new_mapping_df)
        
        updated_mapping_df.write.format("delta").mode("overwrite").save(mapping_table_path)
    else:
        updated_mapping_df = mapping_df
    
    return updated_mapping_df