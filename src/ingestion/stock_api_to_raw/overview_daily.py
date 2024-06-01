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
from src.common.schemas.overview import overview_schema


storage_account = os.environ.get("storage_account")
container = os.environ.get("container")
client_id = os.environ.get("client_id")
service_principal_secrete = os.environ.get("service_principal_secrete")


#output_path = '/Users/azeez/Projects/nvers/src/data'
output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/overview"

key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 500
function='OVERVIEW'
pipe_line_type = 'batch'



schema = overview_schema()
spark = create_spark_session("overview")
spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)

producer = 'p'
overview_processor = StreamProcessor(spark)


async def main():

    async with aiohttp.ClientSession() as session:
        timestamp = ''
        increament = 0
        
        for chunk in utils.chunk_list(symbols, rate_limit):
                batch_data = await utils.fetch_and_produce_batch_2(session, chunk, key, function, timestamp,'',pipe_line_type,'')
                increament += 1
                #print(batch_data)

                #validated_data = [utils.validate_data(record) for record in batch_data]
                rdd = spark.sparkContext.parallelize(batch_data)
                df = spark.createDataFrame(rdd, schema=schema)


                df = df.withColumn("MarketCapitalization", col("MarketCapitalization").cast(IntegerType())) \
                    .withColumn("EBITDA", col("EBITDA").cast(IntegerType())) \
                    .withColumn("PERatio", col("PERatio").cast(DoubleType())) \
                    .withColumn("PEGRatio", col("PEGRatio").cast(DoubleType())) \
                    .withColumn("BookValue", col("BookValue").cast(DoubleType())) \
                    .withColumn("DividendPerShare", col("DividendPerShare").cast(DoubleType())) \
                    .withColumn("DividendYield", col("DividendYield").cast(DoubleType())) \
                    .withColumn("EPS", col("EPS").cast(DoubleType())) \
                    .withColumn("RevenuePerShareTTM", col("RevenuePerShareTTM").cast(DoubleType())) \
                    .withColumn("ProfitMargin", col("ProfitMargin").cast(DoubleType())) \
                    .withColumn("OperatingMargin", col("OperatingMarginTTM").cast(DoubleType())) \
                    .withColumn("ReturnOnAssetsTTM", col("ReturnOnAssetsTTM").cast(DoubleType())) \
                    .withColumn("ReturnOnEquityTTM", col("ReturnOnEquityTTM").cast(DoubleType())) \
                    .withColumn("RevenueTTM", col("RevenueTTM").cast(IntegerType())) \
                    .withColumn("GrossProfitTTM", col("GrossProfitTTM").cast(IntegerType())) \
                    .withColumn("DilutedEPSTTM", col("DilutedEPSTTM").cast(DoubleType())) \
                    .withColumn("QuarterlyEarningsGrowthYOY", col("QuarterlyEarningsGrowthYOY").cast(DoubleType())) \
                    .withColumn("QuarterlyRevenueGrowthYOY", col("QuarterlyRevenueGrowthYOY").cast(DoubleType())) \
                    .withColumn("AnalystTargetPrice", col("AnalystTargetPrice").cast(DoubleType())) \
                    .withColumn("AnalystRatingStrongBuy", col("AnalystRatingStrongBuy").cast(IntegerType())) \
                    .withColumn("AnalystRatingBuy", col("AnalystRatingBuy").cast(IntegerType())) \
                    .withColumn("AnalystRatingHold", col("AnalystRatingHold").cast(IntegerType())) \
                    .withColumn("AnalystRatingSell", col("AnalystRatingSell").cast(IntegerType())) \
                    .withColumn("AnalystRatingStrongSell", col("AnalystRatingStrongSell").cast(IntegerType())) \
                    .withColumn("TrailingPE", col("TrailingPE").cast(DoubleType())) \
                    .withColumn("ForwardPE", col("ForwardPE").cast(DoubleType())) \
                    .withColumn("PriceToSalesRatioTTM", col("PriceToSalesRatioTTM").cast(DoubleType())) \
                    .withColumn("PriceToBookRatio", col("PriceToBookRatio").cast(DoubleType())) \
                    .withColumn("EVToRevenue", col("EVToRevenue").cast(DoubleType())) \
                    .withColumn("EVToEBITDA", col("EVToEBITDA").cast(DoubleType())) \
                    .withColumn("Beta", col("Beta").cast(DoubleType())) \
                    .withColumn("52WeekHigh", col("52WeekHigh").cast(DoubleType())) \
                    .withColumn("52WeekLow", col("52WeekLow").cast(DoubleType())) \
                    .withColumn("50DayMovingAverage", col("50DayMovingAverage").cast(DoubleType())) \
                    .withColumn("200DayMovingAverage", col("200DayMovingAverage").cast(DoubleType())) \
                    .withColumn("SharesOutstanding", col("SharesOutstanding").cast(IntegerType()))


                df = df.withColumn("CurrentDate", current_date()) \
                    .withColumn("year", year(current_date())) \
                    .withColumn("month", month(current_date())) \
                    .withColumn("day", dayofmonth(current_date()))

                df = df.filter(col("Symbol").isNotNull())


                columns = ["Symbol", "CurrentDate"] + [col for col in df.columns if col not in ["Symbol", "CurrentDate"]]
                df = df.select(columns)

                print(f'Executed asyc for batch {increament}')
                await asyncio.sleep(60)  

                overview_processor.ingest_into_raw_zone(df, output_path)
                print(f'Inserted into raw for batch {increament}')

        print('Done executing for all months')
        spark.stop()


if __name__ == "__main__":
    asyncio.run(main())



