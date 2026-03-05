from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, pandas_udf
import pandas as pd
cosmosEndPoint="https://cosmosdb-realstate0.documents.azure.com:443/"
accountKey=dbutils.secrets.get(scope="DBX-realstate-Scope",key="cosmosDB-secret-access-token")
cosmos_config_fixed = {
"spark.cosmos.accountEndpoint": cosmosEndPoint,
  "spark.cosmos.accountKey": accountKey,
  "spark.cosmos.database": "AqarSystem",
  "spark.cosmos.container": "Interactions",
  "spark.cosmos.changeFeed.startFrom": "Now",
  "spark.cosmos.changeFeed.mode": "LatestVersion",
  "spark.cosmos.changeFeed.itemCountPerTriggerHint": "500",
  "spark.cosmos.read.partitioning.strategy": "Restrictive"
  }

BOT_TOKEN = "8541016212:AAEUUd3FYl4SNcogC5SwGbMUt9mkXHJeuuw"
urgent_group_id = "-1003800240930"
sales_group_id = "-1003602749646"  

def send_to_telegram_2(df, batch_id):
    type_msg=None
    import requests
    pdf = df.toPandas()
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    for _, row in pdf.iterrows():
        if row['ai_analysis'] == "urgent":
            target_chat_id = urgent_group_id
            type_msg = 1
        elif row['ai_analysis'] == "sales_lead":
            target_chat_id = sales_group_id
            type_msg = 2
        else :
            continue
        
        icon = "🚨 تنبيه عاجل (صيانة)" if row['ai_analysis'] == "urgent" else "💰 فرصة بيع جديدة"
                
        try:
            if type_msg == 1 :
                message = (
                    f"{icon}\n"
                    f"------------------\n"
                    f"📱 جوال العميل: {row['phone_number']}\n"
                    f"📱 صاحب العقار: {row['user_name']}\n"
                    f"📱  المدينة: {row['city']}\n"
                    f"📱  المنطقة: {row['district']}\n"
                    # f"📱  خط العرض: {row['location_lat']}\n"
                    # f"📱  خط الطول: {row['location_lng']}\n"
                    f"💬 نص الرسالة: {row['comment']}\n"
                    f"⏰ التاريخ: {row['event_time']}"
                )
            else :
                message = (
                    f"{icon}\n"
                    f"------------------\n"
                    f"📱 جوال العميل: {row['phone_number']}\n"
                    f"📱 صاحب العقار: {row['user_name']}\n"
                    f"📱 المساحة : {row['area']}\n"
                    f"📱  السعر: {row['price']}\n"
                    f"📱  المدينة: {row['city']}\n"
                    f"📱  المنطقة: {row['district']}\n"
                    f"💬 نص الرسالة: {row['comment']}\n"
                    f"⏰ التاريخ: {row['event_time']}"
                )

            requests.post(api_url, json={'chat_id': target_chat_id, 'text': message})
        except Exception as e:
            print(f"Error: {e}")


schema = StructType([
    StructField("id", StringType(), True),
    StructField("listing_ref", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("_ts", LongType(), True)
])




urgent_keywords = [
    "صيانة", "تسريب", "خربان", "عاجل", "عاجلة",
    "مشكلة", "الحقوا", "خلل", "مكسور"
]
sales_keywords = [
    "سعر", "كم", "تقسيط", "الدعم السكني",
    "موعد معاينة", "طريقة الدفع", "حجز",
    "متاح", "التسليم"
]
intent_words = ["أريد", "اريد", "أود", "عايز"]

try:
    from transformers import pipeline
    model_id = "CAMeL-Lab/bert-base-arabic-camelbert-mix-sentiment"
    nlp_classifier = pipeline("sentiment-analysis", model=model_id)
except Exception as e:
    print("Error loading model:", e)
    nlp_classifier = None  # fallback

# --- Pandas UDF ---
@pandas_udf("string")
def analyze_batch(comments: pd.Series) -> pd.Series:
    outputs = []
    if nlp_classifier is None:
        return pd.Series(["general"] * len(comments))
    
    results = nlp_classifier(list(comments))
    
    for text, r in zip(comments, results):
        label = r["label"].lower()
        score = r["score"]

        text_lower = text.lower() if text else ""
        
        if any(word in text_lower for word in urgent_keywords):
            outputs.append("urgent")
        elif any(word in text_lower for word in sales_keywords) and label != "negative" or any(word in text_lower for word in intent_words):
            outputs.append("sales_lead")
        elif label == "negative" and score > 0.85:
            outputs.append("complaint")
        else:
            outputs.append("general")

    return pd.Series(outputs)
  
raw_stream = spark.readStream \
  .format("cosmos.oltp.changeFeed") \
  .schema(schema) \
  .options(**cosmos_config_fixed) \
  .load()


streaming_data = raw_stream.select(
    col("id").alias("msg_id"),
    col("listing_ref").cast("int"), 
    col("phone_number"),
    col("comment"),
    col("_ts").cast("timestamp").alias("event_time")
)


streaming_with_ai = streaming_data.withColumn(
    "ai_analysis",
    analyze_batch(col("comment"))
)

posts_df = spark.read.table("realstate.silver.posts")
posts_df.cache()
data_enrichment=streaming_with_ai.join(posts_df,posts_df["row_id"] == streaming_with_ai["listing_ref"],"inner")

filtered_stream = data_enrichment.filter(
                (col("ai_analysis") == "urgent") | (col("ai_analysis") == "sales_lead")
        )\
        .select("msg_id","ai_analysis","comment","event_time","phone_number","user_name","district","city","property_type","area","price")

# display(filtered_stream)

telegram_query = filtered_stream.writeStream \
    .foreachBatch(send_to_telegram_2) \
    .option("checkpointLocation", "/tmp/telegram_checkpoint_1") \
    .trigger(processingTime="5 seconds") \
    .start()

telegram_query.awaitTermination()