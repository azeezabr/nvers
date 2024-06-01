from pyspark.sql import DataFrame
from typing import Callable
from pyspark.sql.functions import from_json, col, expr, struct
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
#from src.ingestion.utils.utils import read_stream

class StreamProcessor:
    def __init__(self, spark):
        self.spark = spark


    def read_stream(self, spark, bootstrap_servers, topic,kafka_username,kafka_password):
        df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
        .option("startingOffsets", "earliest") \
        .load()

        return df


    def process_stream(self,df_f, schema):
        #df = read_stream_func(self.spark, bootstrap_servers, topic,kafka_username,kafka_password)
        df = df_f.selectExpr("CAST(value AS STRING) as json")
        df = df.withColumn("df", from_json(col("json"), schema)).select("df.*")
        
        return df

        #    def process_stream(self, bootstrap_servers: str, topic: str, schema,kafka_username,kafka_password):



    def start_query(self, df: DataFrame, output_path: str, checkpoint_path: str, process_time: str):
        write = df.writeStream \
            .format("json") \
            .outputMode("append") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("year","month", "day", "hour") \
            .trigger(processingTime=f"{process_time}") \
            .start()
        write.awaitTermination()



    def ingest_into_raw_zone(self,parsed_df, output_path):
                parsed_df.write \
                .partitionBy("year","month", "day") \
                .mode("append") \
                .parquet(output_path)

    
