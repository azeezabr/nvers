import asyncio
import aiohttp
import time
import json
import os
from src.common import get_stock_list
from src.common.utils import other_utils
from src.ingestion.utils import utils
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.common.utils.spark_session import create_spark_session, set_spark_config
from src.ingestion.classes.StreamProcessor import StreamProcessor
from src.common.schemas.top_gainers import top_gainers_loser


storage_account = os.environ.get("storage_account")
container = os.environ.get("container")
client_id = os.environ.get("client_id")
service_principal_secrete = os.environ.get("service_principal_secrete")


#output_path = '/Users/azeez/Projects/nvers/src/data'
output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/top-gainers-and-losers"

key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 500
function='TOP_GAINERS_LOSERS'
pipe_line_type = 'batch'



schema = top_gainers_loser()
spark = create_spark_session("top")
spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)

producer = 'p'
overview_processor = StreamProcessor(spark)


async def main():

    async with aiohttp.ClientSession() as session:
        timestamp = ''
        increament = 0
        
        for chunk in utils.chunk_list(symbols, rate_limit):
                batch_data = await utils.fetch_and_produce_batch_2(session, chunk, key, function, timestamp,'',pipe_line_type,'')
            

                rdd = spark.sparkContext.parallelize(batch_data)
                df = spark.createDataFrame(rdd, schema=schema)


                df_gainers = df.withColumn("top_gainers", explode("top_gainers")) \
                    .withColumn("type", lit("gainer")) \
                    .select("metadata", "last_updated", "top_gainers.*", "type")

                df_losers = df.withColumn("top_losers", explode("top_losers")) \
                    .withColumn("type", lit("loser")) \
                    .select("metadata", "last_updated", "top_losers.*", "type")

                df_active = df.withColumn("most_actively_traded", explode("most_actively_traded")) \
                    .withColumn("type", lit("active")) \
                    .select("metadata", "last_updated", "most_actively_traded.*", "type")

                df_combined = df_gainers.union(df_losers).union(df_active)

                df_combined = df_combined \
                    .withColumn("price", col("price").cast(DoubleType())) \
                    .withColumn("change_amount", col("change_amount").cast(DoubleType())) \
                    .withColumn("change_percentage", col("change_percentage").cast(StringType())) \
                    .withColumn("volume", col("volume").cast(LongType())) \
                    .withColumn("last_updated", col("last_updated").cast(TimestampType()))

                df_combined = df_combined.withColumn("CurrentDate", current_date()) \
                    .withColumn("year", year(current_date())) \
                    .withColumn("month", month(current_date())) \
                    .withColumn("day", dayofmonth(current_date()))

                df_combined = df_combined.filter(col("ticker").isNotNull())

                columns = ["ticker", "last_updated", "CurrentDate", "year", "month", "day", "metadata", "type"] + \
                        [col for col in df_combined.columns if col not in ["ticker", "last_updated", "CurrentDate", "year", "month", "day", "metadata", "type"]]
                df_combined = df_combined.select(columns)

                print(f'Executed asyc for batch {increament}')

                overview_processor.ingest_into_raw_zone(df_combined, output_path)
                print(f'Inserted into raw for batch {increament}')
                break

        print('Done executing for all months')
        spark.stop()


if __name__ == "__main__":
    asyncio.run(main())



