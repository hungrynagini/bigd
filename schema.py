from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, ArrayType, BooleanType, TimestampType, LongType,FloatType


memberSchema = StructType([
    StructField("member_id", StringType(), True),
    StructField("photo", StringType(), True),
    StructField("member_name", StringType(), True),
])

eventSchema = StructType([
    StructField("event_name", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("time", LongType(), True),
    StructField("event_url", StringType(), True),
])

groupTopicsScheme = StructType([
    StructField("urlkey", StringType(), True),
    StructField("topic_name", StringType(), True),
])

venueSchema = StructType([
    StructField("venue_name", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("venue_id", StringType(), True),

])

groupSchema = StructType([
    StructField("group_topics", ArrayType(groupTopicsScheme), True),
    StructField("group_city", StringType(), True),
    StructField("group_country", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("group_name", StringType(), True),
    StructField("group_lon", FloatType(), True),
    StructField("group_urlname", StringType(), True),
    StructField("group_state", StringType(), True),
    StructField("group_lat", FloatType(), True),

])

customSchema = StructType([
    StructField("venue", venueSchema, True),
    StructField("visibility", StringType(), True),
    StructField("response", StringType(), True),
    StructField("guests", LongType(), True),
    StructField("member", memberSchema, True),
    StructField("rsvp_id", StringType(), True),
    StructField("mtime", LongType(), True),
    StructField("event", eventSchema, True),
    StructField("group", groupSchema, True)
])
