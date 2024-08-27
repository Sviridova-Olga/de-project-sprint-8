import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql import functions as f

TOPIC_IN = "student.topic.cohort23.sviridova-olga-iccf_in"
TOPIC_OUT = "student.topic.cohort23.sviridova-olga-iccf_out"

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.4.0",
    ]
)

postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan'
}


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    print("Batch_id: ", str(epoch_id))
    
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    df.write \
        .mode("append") \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .options(**postgresql_settings) \
        .option("schema", "public") \
        .option('dbtable', 'subscribers_feedback') \
        .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    df_kafka = (df.select(to_json(struct(col("restaurant_id"),
        col("adv_campaign_id"),
        col("adv_campaign_content"),
        col("adv_campaign_owner"),
        col("adv_campaign_owner_contact"),
        col("adv_campaign_datetime_start"),
        col("adv_campaign_datetime_end"),
        col("datetime_created"),
        col("client_id"),
        col("trigger_datetime_created"),
    )).alias("value")) \
      .select("value"))

    df_kafka = df_kafka.select(col("value").cast(StringType()))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_kafka.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", TOPIC_OUT) \
        .option("truncate", False) \
        .save()

    # очищаем память от df
    df.unpersist()




# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .master("local[*]") \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (spark.readStream \
                             .format('kafka') \
                             .option('kafka.bootstrap.servers', 'localhost:9092') \
                             .option('subscribe', TOPIC_IN) \
                             .option("startingOffsets", "earliest") \
                             .load() \
                             .select(col('value').cast(StringType()),
                                     'timestamp'))

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField('restaurant_id', StringType(), False),
    StructField('adv_campaign_id', StringType(), False),
    StructField('adv_campaign_content', StringType(), True),
    StructField('adv_campaign_owner', StringType(), True),
    StructField('adv_campaign_owner_contact', StringType(), True),
    StructField('adv_campaign_datetime_start', LongType(), True),
    StructField('adv_campaign_datetime_end', LongType(), True),
    StructField('datetime_created', TimestampType(), True)
])

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df.withColumn('value', from_json(col=f.col('value'), schema=incomming_message_schema)) \
    .select(
        col("value.restaurant_id").alias("restaurant_id"),
        col("value.adv_campaign_id").alias("adv_campaign_id"),
        col("value.adv_campaign_content").alias("adv_campaign_content"),
        col("value.adv_campaign_owner").alias("adv_campaign_owner"),
        col("value.adv_campaign_owner_contact").alias("adv_campaign_owner_contact"),
        col("value.adv_campaign_datetime_start").alias("adv_campaign_datetime_start"),
        col("value.adv_campaign_datetime_end").alias("adv_campaign_datetime_end"),
        col("value.datetime_created").alias("datetime_created"),
        col("timestamp"),
        f.round(f.unix_timestamp(f.current_timestamp())).cast("int").alias("current_timestamp_utc")
    ).dropDuplicates(["restaurant_id", "adv_campaign_id"]).withWatermark("timestamp", "10 minutes")) \
    .filter(col("current_timestamp_utc").between(col("adv_campaign_datetime_start"), col("adv_campaign_datetime_end")))

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://localhost:5432/de') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_restaurants') \
    .options(**postgresql_settings) \
    .load() \
    .select('client_id', 'restaurant_id')

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, on="restaurant_id", how="inner") \
    .withColumn("trigger_datetime_created", f.round(f.unix_timestamp(f.current_timestamp())).cast("int")) \
    .select(
    col("restaurant_id"),
    col("adv_campaign_id"),
    col("adv_campaign_content"),
    col("adv_campaign_owner"),
    col("adv_campaign_owner_contact"),
    col("adv_campaign_datetime_start"),
    col("adv_campaign_datetime_end"),
    col("datetime_created").cast("int"),
    col("client_id"),
    col("trigger_datetime_created"))

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
