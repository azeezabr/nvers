# Databricks notebook source
import sys, os, importlib
import importlib
from pyspark.sql.functions import lit, current_date, monotonically_increasing_id,current_date,col, when, expr, to_date, unix_timestamp, from_unixtime, current_timestamp, date_format, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

businessDate = '2024-08-04'
businessDate = to_date(lit(businessDate), 'yyyy-MM-dd')
dividendDate = '2024-06-14' # wil later use businesDate for this.

# COMMAND ----------

paths_and_modules = {
    
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.company_profile'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/': ['utils'],
    f'{dbutils.secrets.get(scope="nvers", key="usr_dir")}/nvers/src/common/schemas/': ['silver.util_func']
}


for path, modules in paths_and_modules.items():
    abs_path = os.path.abspath(path)
    if abs_path not in sys.path:
        sys.path.append(abs_path)
    
    for module in modules:
        globals()[module] = importlib.import_module(module)
        importlib.reload(globals()[module])

import silver.company_profile as sv
import utils
import silver.util_func as util
 
 



# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.azure.account.key.degroup1.dfs.core.windows.net", dbutils.secrets.get('nvers','SID')) 

# COMMAND ----------

storage_name = dbutils.secrets.get('nvers','storage_name')
container_name = dbutils.secrets.get('nvers','container_name')
adls_path = f"abfss://{container_name}@{storage_name}.dfs.core.windows.net"

silver_layer_path = f"{adls_path}/silver" 
sv_company_profile_nm = 'company_profile' 
sv_company_profile_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{sv_company_profile_nm}")
sv_company_profile_df = sv_company_profile_dt.toDF().filter(col('EndDate').isNull())

sv_company_metrics_nm = 'company_symbol_metrics' 
sv_company_metrics_dt = DeltaTable.forPath(spark, f"{silver_layer_path}/{sv_company_metrics_nm}")
sv_company_metrics_df = sv_company_metrics_dt.toDF().filter(col('EndDate') == businessDate)

gold_layer_path = f"{adls_path}/gold" 
gold_table_DimCompany_nm = 'DimCompany' 
gold_table_DimCompany_dt = DeltaTable.forPath(spark, f"{gold_layer_path}/{gold_table_DimCompany_nm}")
gold_table_DimCompany_df = gold_table_DimCompany_dt.toDF().filter(col('IsActive') == 'Y')

 
gold_table_FactDividendPayout_nm = 'FactDividendPayout' 
 


# COMMAND ----------

result_df = sv_company_metrics_df.filter(col('DividendDate') == dividendDate).select('CompanyID', 'DividendDate', 'DividendPerShare', 'DividendYield')

# COMMAND ----------

notebook_path = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath())


result_df = result_df \
    .withColumn("PayoutDate", lit(col('DividendDate').cast("Date"))) \
    .withColumn("AmountPerShare", lit(col('DividendPerShare').cast("double"))) \
    .withColumn("AmountPerSharePerc", lit(col('DividendYield').cast("double"))) \
    .withColumn("DimDateKey", date_format(to_date(col("DividendDate"), 'yyyy-MM-dd'), "yyyyMMdd").cast("int")) \
    .withColumn("ProcessDate", current_timestamp()) \
    .withColumn("JobName", lit(notebook_path))
    

# COMMAND ----------

join_condition = result_df["CompanyId"] == gold_table_DimCompany_df["CompanyId"]

final_df = result_df.join(
    gold_table_DimCompany_df,
    join_condition,
    "inner"
).select(
    result_df["*"],  
    gold_table_DimCompany_df["DimCompanyID"]  
)

# COMMAND ----------

final_df = final_df.withColumn("DimCompanyKey", col("DimCompanyID"))

final_df = final_df.drop("DividendDate", "DividendYield", "DimCompanyID","DividendPerShare")

# COMMAND ----------

final_df.write.format("delta").mode("append").save(f'{gold_layer_path}/{gold_table_FactDividendPayout_nm}')


# COMMAND ----------

gold_table_FactCompany_dt = DeltaTable.forPath(spark, f"{gold_layer_path}/{gold_table_FactDividendPayout_nm}")
#gold_table_FactCompany_df = gold_table_FactCompany_dt.toDF().filter(col('IsActive') == businessDate)
display(gold_table_FactCompany_dt.toDF())

# COMMAND ----------


