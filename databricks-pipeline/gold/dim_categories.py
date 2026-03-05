import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable

def get_unique_catagories(df):
    df.createOrReplaceTempView("catagories")
    unique_df=spark.sql("""
              with rnk_groups as(
                SELECT  property_type,transaction_type,row_number() over(partition by property_type,transaction_type order by property_type) as rnk 
                from catagories
              ) 
                select property_type,transaction_type from rnk_groups where rnk=1
              """)
    return unique_df

lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("category")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)


if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select("property_type","transaction_type")
    uniqure_categories=get_unique_catagories(input_data)
    uniqure_categories.createOrReplaceTempView("source_table")
    try:
        spark.sql("""
            MERGE INTO realstate.gold.dim_categories AS target
            USING source_table as source 
            ON target.property_type = source.property_type and target.transaction_type = source.transaction_type
            WHEN NOT MATCHED 
            THEN INSERT (property_type, transaction_type)
            values (source.property_type,source.transaction_type)
        """)
        try:
            setlastVersionOfSilverMetadata("category",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert new Data with error: ",e)
else:
    print("cities are up to dated")



