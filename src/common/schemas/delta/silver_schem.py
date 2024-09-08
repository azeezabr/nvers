#delta_table_path = f"{silver_layer_path}/{table}"

'''
spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        CompanyId INT,
        Symbol STRING,
        CompanyName STRING,
        CompanyDescription STRING,
        AssetType STRING,
        Exchange STRING,
        Currency STRING,
        Country STRING,
        Sector STRING,
        Industry STRING,
        Address STRING,
        EffectiveDate DATE,
        EndDate DATE,
        IsCurrent STRING
    ) USING DELTA
   


delta_table_path = f"{silver_layer_path}/{silver_table_name}"

spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        CompanyMetricsId BIGINT GENERATED ALWAYS AS IDENTITY,
        CompanyId INT,
        Symbol STRING,
        MarketCapitalization INT,
        SharesOutstanding INT,
        52WeekHigh DOUBLE,
        52WeekLow DOUBLE,
        50DayMovingAverage DOUBLE,
        200DayMovingAverage DOUBLE,
        ReturnOnEquityTTM DOUBLE,
        DividendPerShare DOUBLE,
        DividendYield DOUBLE,
        DividendDate STRING,
        ExDividendDate STRING,
        SnapshotDate DATE,
        EffectiveDate DATE,
        EndDate DATE,
        IsCurrent STRING
    ) USING DELTA
""")
 
delta_table_path = f"{silver_layer_path}/{silver_table_name}"



spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        StockPriceId BIGINT GENERATED ALWAYS AS IDENTITY,
        CompanyId long ,
        Symbol STRING,
        TradeDate TIMESTAMP,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        Volume BIGINT,
        Hour INT,
        TradeYearMonth INT,
        EffectiveDate DATE
    )
    USING DELTA
    --PARTITIONED BY (Symbol, TradeYearMonth)
    CLUSTER BY (Symbol, TradeDate)
""")




delta_table_path = f"{silver_layer_path}/{silver_table_name}"



spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        StockPriceId BIGINT GENERATED ALWAYS AS IDENTITY,
        CompanyId long ,
        Symbol STRING,
        TradeDate DATE,
        Open DOUBLE,
        High DOUBLE,
        Low DOUBLE,
        Close DOUBLE,
        Volume BIGINT,
        TradeYearMonth INT,
        EffectiveDate DATE
    )
    USING DELTA
    PARTITIONED BY (Symbol, TradeYearMonth)
    --CLUSTER BY (Symbol, TradeDate)
""")
  '''