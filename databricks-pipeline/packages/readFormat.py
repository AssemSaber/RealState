# from pyspark.sql import SparkSession
def read_dataFrame(format, layer, table):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    path = f'abfss://{layer}@datalakestorage00real.dfs.core.windows.net/{table}/'
    df = spark.read.format(format).load(path)
    return df