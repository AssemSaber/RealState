# Databricks notebook source
# MAGIC %md
# MAGIC ## you could use spark.conf.set()
# MAGIC ## we read or write directly as we make external location that allow us
# MAGIC

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# DBTITLE 1,Untitled
 
# spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",service_account)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Untitled
import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType,TimestampType
df=spark.read.format('json').load('abfss://bronze@datalakestorage000real.dfs.core.windows.net/posts/')

# COMMAND ----------

df.printSchema()

# COMMAND ----------


display(df)

# COMMAND ----------

unnecessary_columns = ['user.img','user.phone','user.rega_id','uri','title','content','imgs','street_direction','path',"user.img","user.phone","native.external_url","native.title","native.image","native.logo","native.description","native.external_url",'has_extended_details','refresh','width','type','length','street_width']
drop_unnecessay_columns=df.drop(*unnecessary_columns)

# COMMAND ----------

renamed_columns = drop_unnecessay_columns.withColumn('user_name', f.col('`user.name`').cast('string')) \
    .withColumn('user_iam_verified', f.expr("try_cast(`user.iam_verified` as int)")) \
    .withColumn('user_review', f.col('`user.review`').cast('float')) \
    .withColumn('location_lat', f.col('`location.lat`').cast(DecimalType(10, 5))) \
    .withColumn('location_lng', f.col('`location.lng`').cast(DecimalType(10, 5))) \
    .withColumn('create_time', f.col('create_time').cast(TimestampType())) \
    .withColumn('created_at', f.col('createdAt').cast(TimestampType())) \
    .withColumn('updated_at', f.col('updatedAt').cast(TimestampType())) \
    .withColumn('air_condition', f.expr("try_cast(ac as int)")) \
    .withColumn('kitchen', f.expr("try_cast(ketchen as int)")) \
    .withColumn('row_id', f.expr("try_cast(id as int)")) \
    .withColumn('bathrooms', f.expr("try_cast(wc as int)")) \
    .withColumn('bedrooms', f.expr("try_cast(beds as int)")) \
    .withColumn('furnished', f.expr("try_cast(furnished as int)"))\
    .withColumn('livings', f.expr("try_cast(livings as int)"))\
    .withColumn('price', f.col('price').cast('float'))\
    .withColumn('rent_period', f.expr("try_cast(rent_period as int)"))\
    .withColumn('daily_rentable', f.expr("try_cast(daily_rentable as int)"))\
    .drop('user.name', 'user.iam_verified', 'user.review', 'location.lat', 'location.lng', 
          'CreatedAt', 'createdAt', 'updatedAt', 'ac', 'ketchen', 'id', 'wc', 'beds','last_update')

# COMMAND ----------

display(renamed_columns)

# COMMAND ----------

renamed_columns.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Identifying Number of nulls in each column

# COMMAND ----------

import pyspark.sql.functions as f

null_counts = renamed_columns.select([
    f.sum(f.when(f.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in renamed_columns.columns
])

columns_with_nulls = [c for c in null_counts.columns if null_counts.collect()[0][c] > 0]

display(null_counts.select(columns_with_nulls))

# COMMAND ----------

renamed_columns.printSchema()

# COMMAND ----------

display(renamed_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC # Exporation data analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # find relationship why advertiser_type is null

# COMMAND ----------

renamed_columns.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select advertiser_type,user_name,count(*) from posts
                  where group by advertiser_type,user_name having user_name='((((ابوعمر للعقارات ))))' order by advertiser_type,user_name """)
display(explore)

# COMMAND ----------

renamed_columns.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select distinct advertiser_type from posts
                  """)
display(explore)

# COMMAND ----------

# MAGIC %md
# MAGIC ## solution is that
# MAGIC # we'll be able to replace null values with N/A

# COMMAND ----------

replaceNulls_advertiser_type=renamed_columns.fillna('N/A')

# COMMAND ----------

display(replaceNulls_advertiser_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## check air condition column

# COMMAND ----------

replaceNulls_advertiser_type.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select distinct air_condition from posts
                  """)
display(explore)

# COMMAND ----------

replaceNulls_advertiser_type.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select air_condition ,count(*) from posts group by air_condition 
                  """)
display(explore)

# COMMAND ----------

replaceNulls_advertiser_type.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select district, air_condition ,count(*)  from posts GROUP BY district, air_condition
                  order by district
                  """)
display(explore)

# COMMAND ----------

renamed_columns.createOrReplaceTempView("posts")
# display(renamed_columns.count())
explore=spark.sql("""
                  select count(*)  from posts where air_condition='NAN'
                  """)
display(explore)

# COMMAND ----------

# MAGIC %md
# MAGIC ## check age column

# COMMAND ----------

res=spark.sql("""
          select district,age,count(*)as freq from posts group by district,age order by district,age
          """)
display(res)
     

# COMMAND ----------

# MAGIC %md
# MAGIC # the solution is that
# MAGIC ## for each group we select the most frequent type either 0 or 1 
# MAGIC ### حي ابحر الجنوبية	NaN	204
# MAGIC ### حي ابحر الجنوبية	1	4
# MAGIC ### حي ابحر الجنوبية	0	12
# MAGIC ## >> but the group has one district, the value is one ( as business they live in dessert and the temperature is high requriging AC)
# MAGIC ### حي ابها الجديدة	NaN	8

# COMMAND ----------

lookup_age=spark.sql("""
          with freq as(
              select district,age,count(*)as freq from posts group by district,age order by district,age
          ), rnk as(
            select  district,age,freq,ROW_NUMBER() over(partition by district order by freq desc) as rnk_freq from freq    
          ), fillterNull as(
              select district,min(rnk_freq) as r_freq from rnk where age is not null and age <>'NaN' group by district
          )
            select r.district as district_lookup,age as age_lookup from fillterNull f join rnk r on f.r_freq=r.rnk_freq and f.district=r.district 

          """)
print(lookup_age.select(f.isnan('age_lookup')).count()) # appearing null is normal
print(lookup_age.select(f.isnull('age_lookup')).count()) # appearing null is normal
display(lookup_age)


# COMMAND ----------

posts_filled = renamed_columns.join(
    f.broadcast(lookup_age),
    renamed_columns["district"] == lookup_age["district_lookup"],
    "left"
).withColumn(
    "age_filled",f.when(
        (  (~f.col("age_lookup").isNull()) & (~f.isnan(f.col("age_lookup"))) ) |( f.col("age").isNull() & (f.isnan(f.col("age"))) ),
        f.col("age_lookup")
    ).when(
           ( f.isnan(f.col("age_lookup")) | f.col("age_lookup").isNull() ),
           f.lit(1)).otherwise(f.col("age"))
)
display(posts_filled)

# COMMAND ----------


res=posts_filled.select("age_filled").distinct()
display(res)

# COMMAND ----------



# COMMAND ----------

display(renamed_columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC select district,age,count(*) from posts group by district,age order by district,age

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #  the solution is that
# MAGIC ##  we conclude that the best way is to calculate the median for each group by district

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========================
# MAGIC ## checking bedrooms, bathrooms, area, and kitchen
# MAGIC ## ========================

# COMMAND ----------

# MAGIC %md 
# MAGIC ### there is a constrain that if there are nulls in all of these columns like bedrooms,bathrooms,area, and kitchen, the record must be dropped

# COMMAND ----------

renamed_columns.where('bedrooms="NAN" and bathrooms="NAN" and area="NAN" and liviging="NAN"').count()



# COMMAND ----------

# MAGIC %md 
# MAGIC ## ===================
# MAGIC ## validating bedrooms values first
# MAGIC ## ===================
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select district,bedrooms,count(*) from posts group by district,bedrooms  order by district,bedrooms 

# COMMAND ----------

# MAGIC %md
# MAGIC ## the solution is to calculate the median for each group by district

# COMMAND ----------

# MAGIC %md
# MAGIC ## ====================
# MAGIC ## validating bathrooms values
# MAGIC ## ==================== 

# COMMAND ----------

# MAGIC %sql
# MAGIC select district, bathrooms,count(*) from posts group by district, bathrooms  order by district, bathrooms 

# COMMAND ----------

# MAGIC %md
# MAGIC ##the solution is to calculate the median for each group by district

# COMMAND ----------

# MAGIC %md
# MAGIC ## ====================
# MAGIC ## validating area values
# MAGIC ## ==================== 

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(area),min(area) from posts

# COMMAND ----------

# MAGIC %sql
# MAGIC select district, round(avg(area),2) as avg_area from posts group by district  order by district

# COMMAND ----------

the solution is to calculate the median for each group by district

# COMMAND ----------

display(renamed_columns)

# COMMAND ----------


