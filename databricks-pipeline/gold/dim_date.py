from pyspark.sql import functions as F
from datetime import date

table_name = "realstate.gold.dim_dates" 
today = date.today()


try:
    last_date = spark.table(table_name).select(F.coalesce(F.max("date"),F.lit("2022-12-31").cast("date")).alias("date"))
    df_start_date = last_date.withColumn("date",F.expr("date +1"))
    new_start_date=df_start_date.first()["date"]
    print(new_start_date,today)
        
except Exception as e :
    print(f"there is an error with : {e}")

if new_start_date < today:
    print(f"Incremental Load: Adding dates from {new_start_date} to {today}")
    
    new_dates_df = spark.sql(f"""
        SELECT explode(sequence(to_date('{new_start_date}'), current_date(), interval 1 day)) as date
    """)

    incremental_dim = new_dates_df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter"),
        F.dayofweek("date").alias("day_of_week"),
        F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0).alias("is_weekend")
    )

    incremental_dim.write.format("delta").mode("append").saveAsTable(table_name)
else:
    print("date is  up to date")