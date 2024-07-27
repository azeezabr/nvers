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


output_path = '/Users/azeez/Projects/nvers/data'
#output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/stock-data-intraday/timeseries-5m"

key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 73 
output_json = "stock_data.json"
interval = '5min'
function='TIME_SERIES_INTRADAY'
pipe_line_type = 'daily'

#Executed asyc for batch 25 for month 2020-02

schema = monthly_schema()
spark = create_spark_session("re-load_2")
spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)

producer = 'p'
processor = StreamProcessor(spark)

months_set = [
    '2009-12', '2009-11', '2009-10', '2009-09', '2009-08', '2009-07', '2009-06', '2009-05', '2009-04', '2009-03', '2009-02', '2009-01',
    '2008-12', '2008-11', '2008-10', '2008-09', '2008-08', '2008-07', '2008-06', '2008-05', '2008-04', '2008-03', '2008-02', '2008-01',
    '2007-12', '2007-11', '2007-10', '2007-09', '2007-08', '2007-07', '2007-06', '2007-05', '2007-04', '2007-03', '2007-02', '2007-01',
    '2006-12', '2006-11', '2006-10', '2006-09', '2006-08', '2006-07', '2006-06', '2006-05', '2006-04', '2006-03', '2006-02', '2006-01',
    '2005-12', '2005-11', '2005-10', '2005-09', '2005-08', '2005-07', '2005-06', '2005-05', '2005-04', '2005-03', '2005-02', '2005-01',
    '2004-12', '2004-11', '2004-10', '2004-09', '2004-08', '2004-07', '2004-06', '2004-05', '2004-04', '2004-03', '2004-02', '2004-01'
]









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

                #parsed_df.coalesce(1).write.format('json').mode("overwrite").save(f'{output_path_local}/MRNA.json')


                print(f'Executed asyc for batch {increament} for month {m}')
                await asyncio.sleep(60) 

                

                processor.ingest_into_raw_zone(parsed_df, output_path)
                print(f'Inserted into raw for batch {increament} for month {m}')
            print(f'Execution completed for month {m}')

        print('Done executing for all months')
        spark.stop()


if __name__ == "__main__":
    asyncio.run(main())

