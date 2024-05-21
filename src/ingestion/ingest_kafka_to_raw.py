import pyspark
from pyspark.sql import SparkSession
from dbruntime.dbutils import DBUtils

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvault-managed", key="servicePrincipalApplicationId"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault-managed", key="servicePrincipalSecret"),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a7c58d37-1d06-4472-a5e7-7c725497b4be/oauth2/token"
}
 
# Initialize Spark session
spark = SparkSession.builder.appName("tickerlist").getOrCreate()

# Apply the configurations for ADLS Gen2
for config_key, config_value in configs.items():
    spark.conf.set(config_key, config_value)


print('done')

