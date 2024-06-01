from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType,ArrayType)


def top_gainers_loser():
    
        return StructType([
    StructField("metadata", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("top_gainers", ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("price", StringType(), True),
        StructField("change_amount", StringType(), True),
        StructField("change_percentage", StringType(), True),
        StructField("volume", StringType(), True)
    ])), True),
    StructField("top_losers", ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("price", StringType(), True),
        StructField("change_amount", StringType(), True),
        StructField("change_percentage", StringType(), True),
        StructField("volume", StringType(), True)
    ])), True),
    StructField("most_actively_traded", ArrayType(StructType([
        StructField("ticker", StringType(), True),
        StructField("price", StringType(), True),
        StructField("change_amount", StringType(), True),
        StructField("change_percentage", StringType(), True),
        StructField("volume", StringType(), True)
    ])), True)
    ])