#delta_table_path = f"{silver_layer_path}/{table}"

'''
spark.sql(f"""
    CREATE TABLE delta.`{delta_table_path}` (
        CompanyId LONG,
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
        IsCurrent STRING,
        CurrentLoadFlg STRING
    ) USING DELTA
    '''