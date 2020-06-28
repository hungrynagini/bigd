import time
from pyspark.sql.types import StringType, BooleanType, TimestampType
from pyspark.sql import SparkSession, SQLContext
from schema import customSchema
import datetime
from pyspark.sql.functions import col, from_json, explode, lit, array, count, hour, first, \
    to_json, udf, collect_list, struct
from pyspark.sql.functions import max as f_max

if __name__ == '__main__':
    topic = 'meetups'
    kafka_ips = "172.31.64.213:9092,172.31.67.169:9092,172.31.79.226:9092"
    cassandra_ip = '172.31.79.89'
    spark = SparkSession.builder.appName('Meetups').config('spark.cassandra.connection.host', cassandra_ip) \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra9182").getOrCreate()

    spark.conf.set('spark.sql.streaming.checkpointLocation', '/home/ubuntu/checkpoint')
    spark.conf.set('spark.cassandra.output.ignoreNulls', 'true')

    states = spark.read.json('/home/ubuntu/project/states.json')
    countries = spark.read.json('/home/ubuntu/project/countries.json')

    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_ips) \
        .option('topic', topic) \
        .option("subscribe", topic) \
        .load()

    upper = udf(lambda x: x.upper(), StringType())
    current_time = datetime.datetime.now()
    cur_hour = current_time - datetime.timedelta(minutes=current_time.minute, seconds=current_time.second, microseconds=current_time.microsecond)
    timediff = udf(lambda current: current - cur_hour, StringType())
    batch6 = udf(lambda current: cur_hour - datetime.timedelta(hours=6) <= current <= cur_hour + datetime.timedelta(hours=1), BooleanType())
    batch = udf(lambda current: True, BooleanType())
    batch3 = udf(lambda current: cur_hour - datetime.timedelta(hours=3) <= current <= cur_hour + datetime.timedelta(hours=1), BooleanType())
    
    df = df.select(col("key").cast("string"), col("timestamp").cast(TimestampType()),
                from_json(col("value").cast("string"), customSchema).alias('value')) \
        .selectExpr('timestamp', ' value.*').withColumn('time_start', lit(cur_hour.hour)) \
        .join(countries, upper('group.group_country') == countries.code).withColumnRenamed('name', 'country')

    second_task = ds.filter(batch3('timestamp')).filter(col('group.group_country') == 'us') \
    .join(states, col('group.group_state') == states.abbreviation).withColumnRenamed('state_name', 'state') \
    .groupby('state', 'time_start').agg(collect_list('group.group_name').alias('events')) \
    .groupBy('time_start').agg(collect_list(struct('events', 'state')).alias("statistics"))

    first_task = ds.filter(batch6('timestamp')).groupby('country', 'time_start').agg(count(
    'event').alias('number')).groupby("time_start").agg(collect_list(struct('country', 'number')).alias("statistics"))

    third_task = df.filter(batch6('timestamp')).withColumn('group_topics', col('group.group_topics.topic_name')).withColumn("topic_", explode(
    "group_topics")).drop('group_topics').groupby('country', 'time_start', 'topic_').agg(count('event').alias('count')).groupby('time_start', 
    'country').agg(f_max('count').alias("number"), first("topic_").alias("topic")) \
    .groupby('time_start').agg(collect_list(struct("country", 'topic', 'number')).alias("statistics"))
    second_task.write.format("org.apache.spark.sql.cassandra") \
    .options(keyspace='meetups', table='stats_groups') \
    .mode('append') \
    .save()

    third_task.write.format("org.apache.spark.sql.cassandra") \
    .options(keyspace='meetups', table='stats_topics') \
    .mode('append') \
    .save()

    first_task.write.format("org.apache.spark.sql.cassandra") \
    .options(keyspace='meetups', table='stats_events') \
    .mode('append') \
    .save()

