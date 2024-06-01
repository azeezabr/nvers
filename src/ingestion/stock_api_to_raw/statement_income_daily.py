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
from src.common.schemas.income_statement_schema import income_statement_schema


storage_account = os.environ.get("storage_account")
container = os.environ.get("container")
client_id = os.environ.get("client_id")
service_principal_secrete = os.environ.get("service_principal_secrete")


#output_path = '/Users/azeez/Projects/nvers/src/data'
output_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/income-statement"

key = os.environ.get("KEY")

symbols = get_stock_list.get_stock_list()

rate_limit = 500
function='INCOME_STATEMENT'
pipe_line_type = 'batch'



schema = income_statement_schema()
spark = create_spark_session("income_statement")
spark = set_spark_config(spark,storage_account,client_id,service_principal_secrete)

producer = 'p'
income_statement_processor = StreamProcessor(spark)


async def main():

    async with aiohttp.ClientSession() as session:
        timestamp = ''
        increament = 0
        
        for chunk in utils.chunk_list(symbols, rate_limit):
                batch_data = await utils.fetch_and_produce_batch_2(session, chunk, key, function, timestamp,'', pipe_line_type,'')
                increament += 1

                rdd = spark.sparkContext.parallelize(batch_data)
                df = spark.createDataFrame(rdd, schema=schema)

                df_annual = df.withColumn("annualReports", explode("annualReports")) \
                    .withColumn("reportType", lit("annual")) \
                    .select("symbol", "annualReports.*", "reportType")

                df_quarterly = df.withColumn("quarterlyReports", explode("quarterlyReports")) \
                    .withColumn("reportType", lit("quarterly")) \
                    .select("symbol", "quarterlyReports.*", "reportType")

                df_combined = df_annual.union(df_quarterly)

                df_combined = df_combined \
                    .withColumn("grossProfit", col("grossProfit").cast(DoubleType())) \
                    .withColumn("totalRevenue", col("totalRevenue").cast(DoubleType())) \
                    .withColumn("costOfRevenue", col("costOfRevenue").cast(DoubleType())) \
                    .withColumn("costofGoodsAndServicesSold", col("costofGoodsAndServicesSold").cast(DoubleType())) \
                    .withColumn("operatingIncome", col("operatingIncome").cast(DoubleType())) \
                    .withColumn("sellingGeneralAndAdministrative", col("sellingGeneralAndAdministrative").cast(DoubleType())) \
                    .withColumn("researchAndDevelopment", col("researchAndDevelopment").cast(DoubleType())) \
                    .withColumn("operatingExpenses", col("operatingExpenses").cast(DoubleType())) \
                    .withColumn("investmentIncomeNet", col("investmentIncomeNet").cast(DoubleType())) \
                    .withColumn("netInterestIncome", col("netInterestIncome").cast(DoubleType())) \
                    .withColumn("interestIncome", col("interestIncome").cast(DoubleType())) \
                    .withColumn("interestExpense", col("interestExpense").cast(DoubleType())) \
                    .withColumn("nonInterestIncome", col("nonInterestIncome").cast(DoubleType())) \
                    .withColumn("otherNonOperatingIncome", col("otherNonOperatingIncome").cast(DoubleType())) \
                    .withColumn("depreciation", col("depreciation").cast(DoubleType())) \
                    .withColumn("depreciationAndAmortization", col("depreciationAndAmortization").cast(DoubleType())) \
                    .withColumn("incomeBeforeTax", col("incomeBeforeTax").cast(DoubleType())) \
                    .withColumn("incomeTaxExpense", col("incomeTaxExpense").cast(DoubleType())) \
                    .withColumn("interestAndDebtExpense", col("interestAndDebtExpense").cast(DoubleType())) \
                    .withColumn("netIncomeFromContinuingOperations", col("netIncomeFromContinuingOperations").cast(DoubleType())) \
                    .withColumn("comprehensiveIncomeNetOfTax", col("comprehensiveIncomeNetOfTax").cast(DoubleType())) \
                    .withColumn("ebit", col("ebit").cast(DoubleType())) \
                    .withColumn("ebitda", col("ebitda").cast(DoubleType())) \
                    .withColumn("netIncome", col("netIncome").cast(DoubleType()))

                df_combined = df_combined.withColumn("CurrentDate", current_date()) \
                    .withColumn("year", year(current_date())) \
                    .withColumn("month", month(current_date())) \
                    .withColumn("day", dayofmonth(current_date()))

                df_combined = df_combined.filter(col("symbol").isNotNull())

                columns = ["symbol", "fiscalDateEnding", "CurrentDate", "year", "month", "day", "reportType"] + \
                        [col for col in df_combined.columns if col not in ["symbol", "fiscalDateEnding", "CurrentDate", "year", "month", "day", "reportType"]]
                df_combined = df_combined.select(columns)

            
                print(f'Executed asyc for batch {increament}')
                await asyncio.sleep(60)  

                income_statement_processor.ingest_into_raw_zone(df_combined, output_path)
                print(f'Inserted into raw for batch {increament}')

        print('Done executing for all months')
        spark.stop()


if __name__ == "__main__":
    asyncio.run(main())
