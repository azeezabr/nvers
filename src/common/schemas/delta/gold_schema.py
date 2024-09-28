'''

gold_table_path = f"{gold_layer_path}/{gold_layer_path_table_name}"


spark.sql(f"""
    CREATE TABLE delta.`{gold_table_path}` (
        DimCompanyID BIGINT GENERATED ALWAYS AS IDENTITY,
        EffectiveFromDate TIMESTAMP,
        EndToDate TIMESTAMP,
        CompanySymbol STRING,
        CompanyName STRING,
        CompanyDescription STRING,
        AssetType STRING,
        Exchange STRING,
        Currency STRING,
        Country STRING,
        Sector STRING,
        Industry STRING,
        Address STRING,
        CompanyID INT,
        CapCategory STRING,
        FloatCategory STRING,
        IsActive CHAR(1)
    ) USING DELTA
        CLUSTER BY (CompanySymbol)
	""") 
 



 gold_table_path = f"{gold_layer_path}/{gold_table_FactCompany_nm}"



spark.sql(f"""
    CREATE TABLE delta.`{gold_table_path}` (
        FactCompanyID BIGINT GENERATED ALWAYS AS IDENTITY,
        DimCompanyID BIGINT,
        EffectiveFromDate TIMESTAMP,
        EndToDate TIMESTAMP,
        MarketCapitalization INT,
        SharesOutstanding INT,
        52WeekHigh double,
        52WeekLow double,
        50DayMovingAverage double,
        200DayMovingAverage double,
        ReturnOnEquityTTM double,
        DividendPerShare double,
        DividendDateKey INT,
        ExDividendDatekey INT,
        CompanyID INT,
        CompanySymbol STRING,
        ProcessDate TIMESTAMP,
        JobName STRING
    ) USING DELTA
        CLUSTER BY (EffectiveFromDate,CompanySymbol)
	""") 




 
 gold_table_path = f"{gold_layer_path}/{gold_table_FactDividendPayout_nm}"



spark.sql(f"""
    CREATE TABLE delta.`{gold_table_path}` (
        FactDividendPayoutKey BIGINT GENERATED ALWAYS AS IDENTITY,
        DimCompanyKey BIGINT,
        DimDateKey INT,
        AmountPerShare double,
        AmountPerSharePerc double,
        PayoutDate DATE,
        CompanyID INT,
        ProcessDate TIMESTAMP,
        JobName STRING
    ) USING DELTA
        PARTITIONED BY (DimDateKey)
	""") 
 


-- FactStockPriceMonthly


 
gold_table_path = f"{gold_layer_path}/{gold_table_FactStorckPriceDaily_nm}"



spark.sql(f"""
    CREATE TABLE delta.`{gold_table_path}` (
        FactStockPriceMonthlyKey BIGINT GENERATED ALWAYS AS IDENTITY,
        DimCompanyKey BIGINT,
        DimDateKey INT,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        Volume BIGINT,
        TradeYearMonth INT,
        GapUpPerc DOUBLE,
        ProcessDate TIMESTAMP,
        JobName STRING
    ) USING DELTA
        PARTITIONED BY (DimCompanyKey, TradeYearMonth)
	""")
 


--- FactStorckPriceDaily

gold_table_path = f"{gold_layer_path}/{gold_table_FactStorckPriceDaily_nm}"



spark.sql(f"""
    CREATE TABLE delta.`{gold_table_path}` (
        FactStockPriceDailyKey BIGINT GENERATED ALWAYS AS IDENTITY,
        DimCompanyKey BIGINT,
        DimDateKey INT,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        Volume BIGINT,
        TradeYearMonth INT,
        GapUpPerc DOUBLE,
        ProcessDate TIMESTAMP,
        JobName STRING
    ) USING DELTA
        PARTITIONED BY (DimDateKey)
	""")


 '''