import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable

def get_unique_districts(df):
    df.createOrReplaceTempView("districts")
    unique_df=spark.sql("""
              with rnk_groups as(
                SELECT  district_id,district,row_number() over(partition by district_id order by district_id) as rnk 
                from districts
              ) 
                select district_id,district from rnk_groups where rnk=1
              """)
    return unique_df


lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("district")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)


if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select("district_id","district")
    unique_cities=get_unique_districts(input_data)
    unique_cities.createOrReplaceTempView("source_table")
    try:
        spark.sql("""
            MERGE INTO realstate.gold.dim_districts AS target
            USING source_table as source 
            ON target.district_id = source.district_id
            WHEN NOT MATCHED
            THEN INSERT (district_id, district_name)
            values (source.district_id,source.district)
        """)
        try:
            setlastVersionOfSilverMetadata("district",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert new Data with error: ",e)

else:
    print("districts are up to dated")



