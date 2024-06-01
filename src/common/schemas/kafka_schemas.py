from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    TimestampType)

def get_kafka_schema():
    return StructType([
    StructField("symbol", StringType(), False),
    StructField("time", TimestampType(), True),
    StructField("data", StructType([
        StructField("Meta Data", StructType([
            StructField("1. Information", StringType(), True),
            StructField("2. Symbol", StringType(), True),
            StructField("3. Last Refreshed", StringType(), True),
            StructField("4. Interval", StringType(), True),
            StructField("5. Output Size", StringType(), True),
            StructField("6. Time Zone", StringType(), True)
        ]), True),
        StructField("Time Series", StructType([
            StructField("1. open", StringType(), True),
            StructField("2. high", StringType(), True),
            StructField("3. low", StringType(), True),
            StructField("4. close", StringType(), True),
            StructField("5. volume", StringType(), True)
        ]), True)
    ]), True)
])