from math import sin, cos, acos, radians, degrees
from pyspark.sql import *
from pyspark.sql.functions import *


def distance(lat1, lon1, lat2, lon2):
        dist = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
        dist = degrees(dist)
        dist = dist*60*1.1515*1.609344
        return dist


spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCount")\
        .getOrCreate()

lines = spark\
        .readStream\
        .format('socket')\
        .option('host', "localhost")\
        .option('port', 12345)\
        .load()

lines = lines\
    .withColumn("value", split(lines["value"], ','))\

df = lines.select(lines["value"].getItem(0).alias("id").cast("String"),
            lines["value"].getItem(1).alias("vendor_id").cast("String"),
            lines["value"].getItem(2).alias("pickup_datetime").cast("timestamp"),
            lines["value"].getItem(3).alias("dropoff_datetime").cast("timestamp"),
            lines["value"].getItem(4).alias("passenger_count").cast("Integer"),
            lines["value"].getItem(5).alias("pickup_longitude").cast("double"),
            lines["value"].getItem(6).alias("pickup_latitude").cast("double"),
            lines["value"].getItem(7).alias("dropoff_longitude").cast("double"),
            lines["value"].getItem(9).alias("store_and_fwd_flag").cast("String"),
            lines["value"].getItem(8).alias("dropoff_latitude").cast("double"),
            lines["value"].getItem(10).alias("trip_duration").cast("Integer"),
                )

df = df.withColumn("Distance", distance(radians(df["pickup_latitude"]), radians(df["pickup_longitude"]), radians(df["dropoff_latitude"]), radians(df["dropoff_longitude"]).cast("Double")))
df = df.withColumn("Hour", hour(df["pickup_datetime"]).cast('Integer'))
df = df.withColumn("DayOfYear", dayofyear("pickup_datetime").cast('Integer'))
df.printSchema()
#--------------------FIRST STREAMING QUESTION-----------------------------------
#df = df.filter("Distance >=1 AND trip_duration >= 600 AND passenger_count > 2")#edw thelw update
#-------------------SECOND STREAMING QUESTION-----------------------------------
df = df.groupBy(df["Hour"]).count()
#-------------------THIRD STREAMING QUESTION------------------------------------
#df = df.groupBy(df["DayOfYear"], df["vendor_id"], df["Hour"]).count()#.agg(max("count"))
#df = df.agg(max("count"))
#df = df.groupBy(df["hour"], df["vendor_id"]).count()

query = df \
    .writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()

query.awaitTermination()
query.stop()
query.stop()
