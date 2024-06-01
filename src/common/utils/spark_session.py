
from pyspark.sql import SparkSession


def create_spark_session(app_name="DefaultApp"):
    """
    Create and return a Spark session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def set_spark_config(spark,storage_name,client_id,service_principal_secrete):
    spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net", f"{client_id}")
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net",f"{service_principal_secrete}")
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", "https://login.microsoftonline.com/a7c58d37-1d06-4472-a5e7-7c725497b4be/oauth2/token")

    return spark