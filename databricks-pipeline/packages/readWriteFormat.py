from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def read_dataFrame(format, layer, table):
    path = f'abfss://{layer}@datalakestorage000real.dfs.core.windows.net/{table}/'
    df = spark.read.format(format).load(path)
    return df

def getlastVersionOfSilverTable(layer, table): # get from delta log
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forName(spark, f"realstate.{layer}.{table}")
    version=delta_table.history().select("version").first()["version"] # as it is sorted descending
    return version

def getlastVersionOfSilverMetadata(table): # get from table 
    last_version=spark.sql(f"""
                           select last_version from realstate.metadata.goldLayer
                           where table_name='{table}'
                           """).first()["last_version"]
    return last_version

def setlastVersionOfSilverMetadata(table,version): # set into table 
    spark.sql(f"""
              update realstate.metadata.goldLayer 
              set last_version='{version}'
              where table_name='{table}'
              """)

def getlastIngestionOfBronzeMetadata(table): # remeber I don't write in bronze as delta format
    last_ingestion=spark.sql(f"""
              select last_ingestion from realstate.metadata.bronzeLayer 
              where table_name='{table}'
              """).first()["last_ingestion"]
    return last_ingestion

def setlastIngestionOfBronzeMetadata(table,new_last_ingestion): # remeber I don't write in bronze as delta format
    spark.sql(f"""
            update realstate.metadata.bronzeLayer 
            set last_ingestion='{new_last_ingestion}'
            where table_name='{table}'
            """)
def incrementaLoadTable(table,lastVersionMetaData):
    df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", lastVersionMetaData+1) \
    .table(f"realstate.silver.{table}")
    return df









