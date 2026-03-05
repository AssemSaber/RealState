import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable


lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("post")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)


if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select(
        "row_id",
        "user_id",
        "user_review",
        "city_id",
        "district_id",
        "property_type",
        "transaction_type",
        "location_lat",
        "location_lng",
        "created_at",
        "updated_at",
        "has_air_condition",
        "has_kitchen",
        "has_furnished",
        "is_daily_rentable",
        "rent_period",
        "age",
        "bathrooms",
        "bedrooms",
        "livings",
        "area",
        "price"
        )
    display(input_data)
    input_data.createOrReplaceTempView("silver_posts")
    try:
        spark.sql("""
            insert into realstate.gold.fact_posts (
                row_id, 
                user_id ,
                city_id ,
                district_id ,
                date_id ,
                category_id ,
                property_feature_id ,
                user_review ,
                location_lat ,
                location_lng ,
                age,
                bathrooms ,
                bedrooms ,
                livings ,
                area ,
                price 
            )
           select 
                row_id,
                u.id as user_id,
                c.id as city_id,
                d.id as district_id,
                dd.date_key as date_id,
                dc.id as category_id,
                dpf.id as property_feature_id,
                user_review,
                location_lat,
                location_lng,
                age,
                bathrooms,
                bedrooms,
                livings,
                area,
                price
                from silver_posts p left join realstate.gold.dim_users u
                on u.user_id=p.user_id and u.is_active=1 
                left join realstate.gold.dim_cities c 
                on c.city_id=p.city_id
                left join realstate.gold.dim_districts d
                on d.district_id=p.district_id 
                left join realstate.gold.dim_dates dd
                on dd.date=to_date(p.created_at)
                left join realstate.gold.dim_categories dc
                on dc.property_type=p.property_type and dc.transaction_type=p.transaction_type
                left join realstate.gold.dim_property_features dpf
                on dpf.has_air_condition=p.has_air_condition and dpf.has_kitchen=p.has_kitchen and dpf.has_furnished=p.has_furnished and 
                dpf.is_daily_rentable=p.is_daily_rentable and dpf.rent_period=p.rent_period
        """)
        # fact_posts.printSchema()
        try:
            print("inserted new posts")
            setlastVersionOfSilverMetadata("post",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert new Data with error: ",e)

else:
    print("posts are up to dated")



