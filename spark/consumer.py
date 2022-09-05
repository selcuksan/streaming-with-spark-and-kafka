from spark_init import spark, kafka_topic_name, kafka_bootstrap_servers
from pyspark.sql.functions import *
from pyspark.sql.types import *


df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
    .option("subscribe", kafka_topic_name)\
    .option("startingOffsets", "latest")\
    .load()

schema = StructType()\
    .add("date", StringType())\
    .add("id", StringType())\
    .add("first_name", StringType())\
    .add("last_name", StringType())\
    .add("email", StringType())\
    .add("gender", StringType())\
    .add("id_address", StringType())\
    .add("cc", StringType())\
    .add("country", StringType())\
    .add("birthdate", StringType())\
    .add("salary", StringType(),nullable=False)\
    .add("comments", StringType())\
    .add("title", StringType())
schema2 = StructType()\
    .add("a", StringType())\
    .add("b", StringType())
df = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

df = df.select(from_json(col("value"), schema).alias("tablo"))
    
df = df.select("tablo.*")
df = df.groupBy("country").agg({"salary": "sum"})
df = df.select(["country", "sum(salary)"]).orderBy("sum(salary)",ascending=False)
df.printSchema()
df_write_stream = df.writeStream.trigger(processingTime='5 seconds')\
    .outputMode("complete")\
    .option("truncate", "false")\
    .format("console")\
    .start()

df_write_stream.awaitTermination()
