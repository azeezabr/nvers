import sys, os
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def load_bronze_data(spark,base,table_name,format='parquet'):
    path = f"{base}/{table_name}"

    if format.lower() == 'csv':
        return spark.read.format(format).option("header", "true").load(path)
    else:
        return spark.read.format(format).load(path)





def update_symbol_mapping(spark, bronze_df, mapping_table_path, mapping_schema):
    try:
        mapping_df = spark.read.format("delta").load(f'{mapping_table_path}')
    except:
        mapping_df = spark.createDataFrame([], mapping_schema)

    

    # Find new symbols not already in mapping_df
    new_symbols_df = bronze_df.select("Symbol").distinct().subtract(mapping_df.select("Symbol").distinct())
   
    if new_symbols_df.count() > 0:
        # Get the maximum existing CompanyId
        max_company_id_row = mapping_df.agg(F.max("CompanyId")).collect()[0][0]
        max_company_id = max_company_id_row if max_company_id_row is not None else 0

       
        # Assign new CompanyIds starting from max_company_id + 1
        window = Window.orderBy("Symbol")
        new_mapping_df = new_symbols_df.withColumn(
            "CompanyId", F.row_number().over(window) + max_company_id
        ).withColumn("EffectiveDate", F.current_date())

        
        # Combine existing and new mappings
        updated_mapping_df = mapping_df.union(new_mapping_df)

        # Overwrite the mapping table with the updated data
        updated_mapping_df.write.format("delta").mode("overwrite").save(f'{mapping_table_path}')
    else:
        updated_mapping_df = mapping_df

    return updated_mapping_df



def read_delta_to_df(spark,path):
    silver_table_dt = DeltaTable.forPath(spark, f"{path}")
    return silver_table_dt.toDF()


    