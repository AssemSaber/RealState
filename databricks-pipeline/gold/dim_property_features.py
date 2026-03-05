import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable

def get_unique_property_features(df):
    df.createOrReplaceTempView("property_features")
    unique_df=spark.sql("""
              with rnk_groups as(
                SELECT  has_air_condition,has_kitchen,has_furnished,is_daily_rentable,
                rent_period,
                row_number() over(partition by  has_air_condition,has_kitchen,has_furnished,is_daily_rentable,
                rent_period order by has_air_condition) as rnk 
                from property_features
              ) 
                select has_air_condition,has_kitchen,has_furnished,is_daily_rentable,
                rent_period from rnk_groups where rnk=1
              """)
    return unique_df

lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("property_features")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)


if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select("has_air_condition","has_kitchen","has_furnished","is_daily_rentable","rent_period")
    unique_features=get_unique_property_features(input_data)
    unique_features.createOrReplaceTempView("source_table")
    try:
        spark.sql("""
            MERGE INTO realstate.gold.dim_property_features AS target
            USING source_table as source 
            ON target.has_air_condition = source.has_air_condition and target.has_kitchen = source.has_kitchen
            and target.has_furnished = source.has_furnished and target.is_daily_rentable = source.is_daily_rentable
            and target.rent_period = source.rent_period
            WHEN NOT MATCHED 
            THEN INSERT (has_air_condition,has_kitchen,has_furnished,is_daily_rentable,rent_period)
            values (source.has_air_condition,source.has_kitchen,source.has_furnished,source.is_daily_rentable,source.rent_period)
        """)
        try:
            setlastVersionOfSilverMetadata("property_features",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert new Data with error: ",e)
else:
    print("property_features are up to dated")



