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




 
 


 '''