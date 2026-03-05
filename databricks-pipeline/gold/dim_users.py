import sys
sys.path.append('../') # get out from the that path to see the packages
from packages.readWriteFormat import getlastVersionOfSilverTable,getlastVersionOfSilverMetadata,setlastVersionOfSilverMetadata,incrementaLoadTable

def get_unique_users(df):
    df.createOrReplaceTempView("users")
    unique_users=spark.sql("""
              with rnk_groups as(
                SELECT  user_id,user_name,advertiser_type,is_verified,row_number() over(partition by user_id order by user_id) as rnk 
                from users
              ) 
                select user_id,user_name,advertiser_type,is_verified from rnk_groups where rnk=1
              """)
    return unique_users

def users_SCD(df):
    df.createOrReplaceTempView("users")
    spark.sql("""
            MERGE INTO realstate.gold.dim_users AS target
            USING users as source 
            ON target.user_id = source.user_id and  target.is_active = 1
            WHEN MATCHED and 
                (
                    source.user_name<>target.user_name or source.advertiser_type<>target.advertiser_type or source.is_verified<>target.is_verified
                )
            THEN update set target.is_active=0 
        """)
    spark.sql("""
                  insert into realstate.gold.dim_users (user_id, user_name, advertiser_type, is_verified, is_active)
                  select s.user_id,s.user_name,s.advertiser_type,s.is_verified,1 
                  from users s left join realstate.gold.dim_users t on t.user_id=s.user_id
                  and t.is_active = 1
                  where t.user_id is null
                  """)
        
lastVersionTable=getlastVersionOfSilverTable("silver","posts")
lastVersionMetaData=getlastVersionOfSilverMetadata("users")
print("lastVersionTable: ",lastVersionTable)
print("lastVersionMetaData: ",lastVersionMetaData)

if lastVersionMetaData < lastVersionTable:
    load_data = incrementaLoadTable("posts",lastVersionMetaData)
    input_data=load_data.select("user_id","user_name","advertiser_type","is_verified")
    try:
        df_users=get_unique_users(input_data)
        new_users=users_SCD(df_users)
        try:
            setlastVersionOfSilverMetadata("users",lastVersionTable)
        except Exception as e:
            print("Can't update MetaData with error: ",e)
    except Exception as e:
            print("Can't insert/update new Data with error: ",e)
    
else:
    print("users are up to dated")