from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType,
    DateType
)

def company_profile_schema(): 
    return StructType([
        StructField("PrimaryKey", StringType(), False),
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