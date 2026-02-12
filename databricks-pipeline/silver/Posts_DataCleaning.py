
import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType,TimestampType
import sys
sys.path.append('../') # get out from the that path to see the packages
import packages.config  ## credentianl config
from packages.readFormat import read_dataFrame

df=read_dataFrame('json','bronze','posts')

# drop columns
unnecessary_columns = ['user.img','user.phone','user.rega_id','uri','title','content','imgs','street_direction','path',"user.img","user.phone","native.external_url","native.title","native.image","native.logo","native.description","native.external_url",'has_extended_details','refresh','width','type','length','street_width']
drop_unnecessay_columns=df.drop(*unnecessary_columns)


# renamed columns

renamed_columns = drop_unnecessay_columns.withColumn('user_name', f.col('`user.name`').cast('string')) \
    .withColumn('user_iam_verified', f.col('`user.iam_verified`').cast('int')) \
    .withColumn('user_review', f.col('`user.review`').cast(DecimalType(10, 2))) \
    .withColumn('location_lat', f.col('`location.lat`').cast(DecimalType(10, ))) \
    .withColumn('location_lng', f.col('`location.lng`').cast(DecimalType(10, 5))) \
    .withColumn('create_time', f.col('create_time').cast(TimestampType())) \
    .withColumn('created_at', f.col('createdAt').cast(TimestampType())) \
    .withColumn('updated_at', f.col('updatedAt').cast(TimestampType())) \
    .withColumn('air_condition', f.col('ac').cast('int')) \
    .withColumn('kitchen', f.col('ketchen').cast('int')) \
    .withColumn('row_id', f.col('id').cast('int')) \
    .withColumn('bathrooms', f.col('wc').cast('int')) \
    .withColumn('bedrooms', f.col('beds').cast('int')) \
    .withColumn('furnished', f.col('furnished').cast('int'))\
    .withColumn('livings', f.col('livings').cast('int'))\
    .withColumn('price', f.col('price').cast('float'))\
    .withColumn('rent_period', f.col('rent_period').cast('int'))\
    .withColumn('daily_rentable', f.col('daily_rentable').cast('int'))\
    .drop('user.name', 'user.iam_verified', 'user.review', 'location.lat', 'location.lng', 
          'CreatedAt', 'createdAt', 'updatedAt', 'ac', 'ketchen', 'id', 'wc', 'beds','last_update')
# print("final")
display(renamed_columns)
