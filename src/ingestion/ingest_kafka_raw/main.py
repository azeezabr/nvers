from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
from src.common.utils.spark_session import create_spark_session, set_spark_config
from src.ingestion.classes.StreamProcessor import StreamProcessor
from src.ingestion.utils.utils import read_stream
from src.common.schemas.kafka_schemas import get_kafka_schema
import os




kafka_bootstrap_servers = os.environ.get("kafka_bootstrap_servers")
kafka_topic = os.environ.get("kafka_topic")
kafka_username = os.environ.get("kafka_username")
kafka_password = os.environ.get("kafka_password")
storage_account = os.environ.get("storage_account")
container = os.environ.get("container")
client_id = os.environ.get("client_id")
service_principal_secrete = os.environ.get("service_principal_secrete")



output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/landing"
checkpoint_path =f'{output_path}/checkpoint'
process_time = '1 minute'



def main():
    schema = get_kafka_schema()

    spark = create_spark_session("intraDayStream")
    spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)
    processor = StreamProcessor(spark)

    intraday_kafka_df = processor.read_stream(spark, kafka_bootstrap_servers, kafka_topic,kafka_username,kafka_password)

    #intraday_kafka_df = processor.process_stream(read_stream, kafka_bootstrap_servers, kafka_topic, schema, kafka_username,kafka_password)
    df = intraday_kafka_df.selectExpr("CAST(value AS STRING) as json")
    df = df.withColumn("df", from_json(col("json"), schema)).select("df.*")

    renamed_df = df \
            .withColumn("datetime", date_format(col("time"), 'yyyy-MM-dd HH:mm:ss')) \
            .withColumn("year", year(col("time"))) \
            .withColumn("month", month(col("time"))) \
            .withColumn("day", dayofmonth(col("time"))) \
            .withColumn("hour", hour(col("time"))) \
            .select( \
                          col("symbol"), \
                          col("time"), \
                          col("data.Meta Data.`4. Interval`").alias("Interval"), \
                          col("data.Meta Data.`3. Last Refreshed`").cast(TimestampType()).alias("LastRefreshed"), \
                          col("data.Time Series.`1. open`").cast(FloatType()).alias("open"), \
                          col("data.Time Series.`2. high`").cast(FloatType()).alias("high"), \
                          col("data.Time Series.`3. low`").cast(FloatType()).alias("low"), \
                          col("data.Time Series.`4. close`").cast(FloatType()).alias("close"), \
                          col("data.Time Series.`5. volume`").cast(IntegerType()).alias("volume"), \
                            col("year"), \
                            col("month"), \
                            col("day"), \
                            col("hour"))
    
    
    processor.start_query(renamed_df, output_path, checkpoint_path,process_time)
  
if __name__ == "__main__":
    main()
