from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, SQLContext
from schema import customSchema
from pyspark.sql.functions import col, from_json, to_json, udf, struct

if __name__ == '__main__':
    topic = 'meetups'
    kafka_ips = "172.31.64.213:9092,172.31.67.169:9092,172.31.79.226:9092"
    cassandra_ip = '172.31.72.233'
    spark = SparkSession.builder.appName('Meetups').config('spark.cassandra.connection.host', cassandra_ip) \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra9182").getOrCreate()

    spark.conf.set('spark.sql.streaming.checkpointLocation', '/home/ubuntu/checkpoint')
    spark.conf.set('spark.cassandra.output.ignoreNulls', 'true')

    countries = spark.read.json('/home/ubuntu/project/countries.json')
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_ips) \
        .option('topic', topic) \
        .option("subscribe", topic) \
        .load()
    upper = udf(lambda x: x.upper(), StringType())

    ds = df.select(col("key").cast("string"), col("timestamp").cast("timestamp"),
                   from_json(col("value").cast("string"), customSchema).alias('value')) \
        .selectExpr('timestamp', ' value.*') \
        .join(countries, upper('group.group_country') == countries.code)
    #   .selectExpr("to_json(struct(*)) AS value") \

    countries_cities = ds.select(col('name').alias('country'), col('group.group_city').alias("city")) \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace='meetups', table='countries') \
        .start()

    events = ds.select(col('event.event_id').alias('event_id'), col('event.event_name').alias('event_name'),
                       col('event.time').alias('event_time'), col("group.group_name").alias("group_name"),
                       col('name').alias('country'), col('group.group_city').alias("city"),
                       col('group.group_topics.topic_name')) \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace='meetups', table='events') \
        .start()
    city_groups = ds.select(col('group.group_id').alias("group_id"), col('group.group_name').alias('group_name'),
                            col('group.group_city').alias('city_name')) \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace='meetups', table='city_groups') \
        .start()
    groups = ds.select(col('event.event_id').alias('event_id'), col('event.event_name').alias('event_name'),
                       col('event.time').alias('event_time'), col('group.group_id').alias('group_id'),
                       col("group.group_name").alias("group_name"), col('name').alias('country'),
                       col('group.group_city').alias("city"),
                       col('group.group_topics.topic_name')) \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace='meetups', table='groups') \
        .start()
    spark.streams.awaitAnyTermination()
