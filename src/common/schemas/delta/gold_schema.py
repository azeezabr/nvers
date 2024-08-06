'''

delta_table_path = f"{gold_layer_path}/{gold_layer_path_table_name}"



spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
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
        IsActive CHAR(1)
    ) USING DELTA
        CLUSTER BY (CompanySymbol)
	""") 
 


 '''