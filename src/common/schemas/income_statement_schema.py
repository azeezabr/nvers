from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    ArrayType)


def income_statement_schema():
        report_schema = StructType([
            StructField("fiscalDateEnding", StringType(), True),
            StructField("reportedCurrency", StringType(), True),
            StructField("grossProfit", StringType(), True),
            StructField("totalRevenue", StringType(), True),
            StructField("costOfRevenue", StringType(), True),
            StructField("costofGoodsAndServicesSold", StringType(), True),
            StructField("operatingIncome", StringType(), True),
            StructField("sellingGeneralAndAdministrative", StringType(), True),
            StructField("researchAndDevelopment", StringType(), True),
            StructField("operatingExpenses", StringType(), True),
            StructField("investmentIncomeNet", StringType(), True),
            StructField("netInterestIncome", StringType(), True),
            StructField("interestIncome", StringType(), True),
            StructField("interestExpense", StringType(), True),
            StructField("nonInterestIncome", StringType(), True),
            StructField("otherNonOperatingIncome", StringType(), True),
            StructField("depreciation", StringType(), True),
            StructField("depreciationAndAmortization", StringType(), True),
            StructField("incomeBeforeTax", StringType(), True),
            StructField("incomeTaxExpense", StringType(), True),
            StructField("interestAndDebtExpense", StringType(), True),
            StructField("netIncomeFromContinuingOperations", StringType(), True),
            StructField("comprehensiveIncomeNetOfTax", StringType(), True),
            StructField("ebit", StringType(), True),
            StructField("ebitda", StringType(), True),
            StructField("netIncome", StringType(), True)
        ])


        income_statement_schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("annualReports", ArrayType(report_schema), True),
            StructField("quarterlyReports", ArrayType(report_schema), True)
        ])
        return income_statement_schema
