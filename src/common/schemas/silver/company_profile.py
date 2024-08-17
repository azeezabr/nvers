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
        StructField("Symbol", StringType(), False), 
        StructField("CompanyId", IntegerType(), False),
        StructField("EffectiveDate", DateType(), True), 
    ])