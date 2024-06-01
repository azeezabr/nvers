import asyncio
import aiohttp
import time
import json
import os
from src.common import get_stock_list
from src.common.utils import other_utils
from src.ingestion.utils import utils
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, FloatType, TimestampType, StructField, StructType
from src.common.utils.spark_session import create_spark_session, set_spark_config
from src.ingestion.classes.StreamProcessor import StreamProcessor
from src.common.schemas.monthly_intraday_schema import monthly_schema


storage_account = os.environ.get("storage_account")
container = os.environ.get("container")
client_id = os.environ.get("client_id")
service_principal_secrete = os.environ.get("service_principal_secrete")


#output_path = '/Users/azeez/Projects/nvers/src/data'
output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/timeseries-intraday-5m"

key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()#[:1]

rate_limit = 500 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
pipe_line_type = 'daily'



schema = monthly_schema()
spark = create_spark_session("march")
spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)

producer = 'p'
processor = StreamProcessor(spark)

months_set = ['2018-06', '2018-05', '2018-04', '2018-03', '2018-02', 
 '2018-01', '2017-12', '2017-11', '2017-10', '2017-09', '2017-08', 
 '2017-07', '2017-06', '2017-05', '2017-04', '2017-03', '2017-02', 
 '2017-01', '2016-12', '2016-11', '2016-10', '2016-09', '2016-08', 
 '2016-07', '2016-06', '2016-05', '2016-04', '2016-03', '2016-02', 
 '2016-01', '2015-12']





async def main():

    async with aiohttp.ClientSession() as session:
        for m in months_set:
            timestamp = ''
            increament = 0

            for chunk in utils.chunk_list(symbols, rate_limit):
                batch_data = await utils.fetch_and_produce_batch_2(session, chunk, key, function, timestamp, interval, pipe_line_type, m)
                increament += 1

                validated_data = [utils.validate_data(record) for record in batch_data]
                rdd = spark.sparkContext.parallelize(validated_data)
                df = spark.createDataFrame(rdd, schema=schema)

                exploded_df = df.select(
                    col("symbol"),
                    col("time"),
                    col("data.Meta Data"),
                    explode(col("data.Time Series (5min)")).alias("timestamp", "values")
                )

                parsed_df = exploded_df \
                    .withColumn("year", year(col("timestamp"))) \
                    .withColumn("month", month(col("timestamp"))) \
                    .withColumn("day", dayofmonth(col("timestamp"))) \
                    .withColumn("hour", hour(col("timestamp"))) \
                    .select(
                        col("symbol"),
                        col("time").alias("refresh_time"),
                        col("timestamp").alias("timestamp"),
                        col("values.`1. open`").alias("open"),
                        col("values.`2. high`").alias("high"),
                        col("values.`3. low`").alias("low"),
                        col("values.`4. close`").alias("close"),
                        col("values.`5. volume`").alias("volume"),
                        col("year"),
                        col("month"),
                        col("day"),
                        col("hour")
                    )

                print(f'Executed asyc for batch {increament} for month {m}')
                await asyncio.sleep(60)  

                processor.ingest_into_raw_zone(parsed_df, output_path)
                print(f'Inserted into raw for batch {increament} for month {m}')
            print(f'Execution completed for month {m}')

        print('Done executing for all months')
        spark.stop()


if __name__ == "__main__":
    asyncio.run(main())

