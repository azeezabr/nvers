from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    TimestampType,
    FloatType,
    IntegerType,
    MapType)


def monthly_schema():
        
    # Define the schema for the "Time Series (5min)" data
    time_series_schema = StructType([
        StructField("1. open", StringType(), True),
        StructField("2. high", StringType(), True),
        StructField("3. low", StringType(), True),
        StructField("4. close", StringType(), True),
        StructField("5. volume", StringType(), True)
    ])

    # Define the schema for the "Meta Data"
    meta_data_schema = StructType([
        StructField("1. Information", StringType(), True),
        StructField("2. Symbol", StringType(), True),
        StructField("3. Last Refreshed", StringType(), True),
        StructField("4. Interval", StringType(), True),
        StructField("5. Output Size", StringType(), True),
        StructField("6. Time Zone", StringType(), True)
    ])

    # Define the full schema
    data_schema = StructType([
        StructField("Meta Data", meta_data_schema, True),
        StructField("Time Series (5min)", MapType(StringType(), time_series_schema), True)
    ])

    # Define the schema for the top-level structure
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("time", StringType(), True),
        StructField("data", data_schema, True)
    ])

    return schema