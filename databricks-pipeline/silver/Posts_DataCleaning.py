import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType,TimestampType
from datetime import datetime
from pyspark.sql.window import Window
import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import read_dataFrame,getlastIngestionOfBronzeMetadata,setlastIngestionOfBronzeMetadata

def get_freq_lookup(df, first_column, second_column):

    window_spec = Window.partitionBy(first_column).orderBy(f.desc("freq"))
    
    lookup = df.filter(f.col(second_column).isNotNull()) \
               .groupBy(first_column, second_column) \
               .agg(f.count("*").alias("freq"))
               
    result = lookup.withColumn("rnk", f.row_number().over(window_spec)) \
                   .filter(f.col("rnk") == 1) \
                   .select(
                       f.col(first_column).alias(first_column + "_lookup"),
                       f.col(second_column).alias(second_column + "_lookup")
                   )
    
    return result

def fill_missing_with_lookup(big_table, first_column, missing_column):
    lookup_table = get_freq_lookup(big_table, first_column, missing_column)
    lookup_table.persist()
    lookup_col_name = missing_column + "_lookup"
    district_lookup_col = first_column + "_lookup"
    
    joined = big_table.join(
        f.broadcast(lookup_table),
        big_table[first_column] == lookup_table[district_lookup_col],
        "left"
    )
    
    final_df = joined.withColumn(
        missing_column,
        f.coalesce(f.col(missing_column), f.col(lookup_col_name), f.lit(1))
    ).drop(district_lookup_col, lookup_col_name)
    
    lookup_table.unpersist()
    
    return final_df

def get_median_column_optimized(df, column):
    lookup_col = f"median_{column}"
    
    lookup_table = df.filter(f.col(column).isNotNull()) \
                     .groupBy("district") \
                     .agg(f.percentile_approx(column, 0.5).alias(lookup_col)) \
                     .withColumnRenamed("district", "district_lookup")
    
    result = df.join(f.broadcast(lookup_table), 
                    df["district"] == lookup_table["district_lookup"], 
                    "left") \
               .withColumn(column, f.coalesce(f.col(column), f.col(lookup_col), f.lit(500))) \
               .drop("district_lookup", lookup_col)
    
    return result


def meaningful_category(df):
    category_lookup = [
        (1, "Apartment", "Rental"),
        (2, "Land", "Sell"),
        (3, "Villa", "Sell"),
        (4, "Floor", "Rental"),
        (5, "Villa", "Rental"),
        (6, "Apartment", "Sell"),
        (7, "Building", "Sell"),
        (8, "Store", "Rental"),
        (9, "House", "Sell"),
        (10, "Esterahah", "Sell"),
        (11, "House", "Rental"),
        (12, "Farm", "Sell"),
        (13, "Esterahah", "Rental"),
        (14, "Office", "Rental"),
        (15, "Land", "Rental"),
        (16, "Building", "Rental"),
        (17, "Warehouse", "Rental"),
        (18, "Campsite", "Rental"),
        (19, "Room", "Rental"),
        (20, "Store", "Sell"),
        (21, "Furnished Apartment", "Rental"),
        (22, "Floor", "Sell"),
        (23, "Chalet", "Rental")
    ]
    lookup_category = spark.createDataFrame(category_lookup, ["category_id", "property_type", "transaction_type"])
    print("last")
    return df.join(f.broadcast(lookup_category), on=lookup_category["category_id"]==df["category"], how="left").drop("category_id","category")


import pyspark.sql.functions as f

def meaningful_rent_period(df):
    return df.withColumn(
        "rent_period",
        f.when(f.col("rent_period").isin(0, 3), "Yearly")
         .when(f.col("rent_period") == 1, "Daily")
         .when(f.col("rent_period") == 2, "Monthly")
         .otherwise(None) 
    )

def BinaryToString(df,newColumn,oldColumn):
    return df.withColumn(
        newColumn,
        f.when(f.col(oldColumn) == 1, "Yes")
         .when(f.col(oldColumn) == 0, "No")
    ).drop(oldColumn)









# ====================== Main entry ponit =======================#
last_ingestion=getlastIngestionOfBronzeMetadata("posts")

input_data =read_dataFrame('json', 'bronze', 'posts')
new_ingestion=input_data.withColumn("ingestion_time",   
     f.date_format(
            f.to_date(
                f.concat_ws("-", "year", "month", "day")
            ),
            "yyyyMMdd"
        )
    ).agg(f.max("ingestion_time").alias("max_ingestion")).collect()[0]['max_ingestion']

print(f"the last ingestion is :{last_ingestion} and new ingestion is :{new_ingestion} ")
if (last_ingestion<new_ingestion):
        
    raw_df = input_data
    cleaned_df = raw_df.select(
        f.col('user_id'),
        f.col('`user.name`').cast('string').alias('user_name'),
        f.expr("try_cast(`user.iam_verified` as int)").alias('user_iam_verified'),
        f.col('`user.review`').cast('float').alias('user_review'),
        f.col('`location.lat`').cast(DecimalType(10, 5)).alias('location_lat'),
        f.col('`location.lng`').cast(DecimalType(10, 5)).alias('location_lng'),
        f.col('create_time').cast(TimestampType()).alias('create_time'),
        f.col('createdAt').cast(TimestampType()).alias('created_at'),
        f.col('updatedAt').cast(TimestampType()).alias('updated_at'),
        f.col('age').cast("int"),
        f.expr("try_cast(ac as int)").alias('air_condition'),
        f.expr("try_cast(ketchen as int)").alias('kitchen'),
        f.expr("try_cast(wc as int)").alias('bathrooms'),
        f.expr("try_cast(beds as int)").alias('bedrooms'),
        f.expr("try_cast(livings as int)").alias('livings'),
        f.expr("try_cast(furnished as int)").alias('is_furnished'),
        f.expr("try_cast(id as int)").alias('row_id'),
        f.col('price').cast('float').alias('price'),
        f.expr("try_cast(rent_period as int)").alias('rent_period'),
        f.expr("try_cast(daily_rentable as int)").alias('daily_rentable'),
        f.col('district'),
        f.col('category'),
        f.col('area'),
        f.col('city'),
        f.col('advertiser_type'),
        f.col('city_id'),
        f.col('district_id')

    )


    df = cleaned_df.repartition("district").persist()

    df = meaningful_category(df)
    df = df.fillna({'user_review': 0, 'advertiser_type': 'N/A'})

    df = get_median_column_optimized(df, "area")

    columns_to_fix = ['age', 'air_condition', 'bedrooms', 'bathrooms','rent_period', 'kitchen', 'is_furnished', 'livings','daily_rentable']

    last_saved_df = df # we need to hold that dataframe 
    for col_name in columns_to_fix:
        print(f"Processing {col_name} at {datetime.now()}")
        df = fill_missing_with_lookup(df, "district", col_name) # in the first loop, df is last computation before loop(df=get_median..)
        
        if col_name in ['bedrooms', 'is_furnished']:
                df.persist() # 
                df.count()   # 
                
                if last_saved_df is not None:
                    last_saved_df.unpersist() #  we will remove the old to realse memory (in case of bedrooms we realse before the loop cached)
                last_saved_df = df # we hold the last one 

    with_rent_period = meaningful_rent_period(df)
    df_final=with_rent_period 
    binary=["user_iam_verified","is_furnished","air_condition","kitchen","daily_rentable"]
    stringOfBinary=["is_verified","has_furnished","has_air_condition","has_kitchen","is_daily_rentable"]
    for column in range(len(binary)):
        df_final=BinaryToString(df_final,stringOfBinary[column],binary[column])


    print(f"final_done: {datetime.now()}")
    df_final.printSchema()
    selected=df_final.select(
        "row_id",
        "user_id",
        "user_name",
        "advertiser_type",
        "is_verified",
        "user_review",
        "city_id",
        "city",
        "district_id",
        "district",
        "property_type",
        "transaction_type",
        "location_lat",
        "location_lng",
        "create_time",
        "created_at",
        "updated_at",
        "age",
        "has_air_condition",
        "has_kitchen",
        "bathrooms",
        "bedrooms",
        "livings",
        "area",
        "has_furnished",
        "price",
        "rent_period",
        "is_daily_rentable"
        )
    try:
        selected.coalesce(5).write.format("delta").mode("append").saveAsTable("realstate.silver.posts")
        setlastIngestionOfBronzeMetadata('posts',new_ingestion)
    except Exception as e:
        print(f"We're unable to write due to {e}")
else :
    print("posts is up to date")
