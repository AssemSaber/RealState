# we must setup sparksession in files,not like noot books
from pyspark.sql import SparkSession
import sys
sys.path.append('../') 
from packages.credential import get_storage_account,get_application_id,get_directory_id,get_service_creddential
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

storage_account = get_storage_account()
application_id = get_application_id()
directory_id = get_directory_id()
service_credintial=get_service_creddential()

# print(storage_account,application_id,directory_id,service_credintial)
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credintial)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
