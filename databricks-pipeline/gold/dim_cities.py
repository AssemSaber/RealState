import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable


def get_unique_cities(df):
    df.createOrReplaceTempView("cities")
    unique_df=spark.sql("""
              with rnk_groups as(
                SELECT  city_id,city,row_number() over(partition by city_id order by city_id) as rnk 
                from cities
              ) 
                select city_id,city from rnk_groups where rnk=1
              """)
    return unique_df

lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("city")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)


if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select("city_id","city")
    unique_cities=get_unique_cities(input_data)
    unique_cities.createOrReplaceTempView("source_table")
    try:
        spark.sql("""
            MERGE INTO realstate.gold.dim_cities AS target
            USING source_table as source 
            ON target.city_id = source.city_id
            WHEN NOT MATCHED 
            THEN INSERT (city_id, city_name)
            values (source.city_id,source.city)
        """)
        try:
            setlastVersionOfSilverMetadata("city",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert new Data with error: ",e)
else:
    print("cities are up to dated")



