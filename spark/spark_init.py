from pyspark.sql import SparkSession

kafka_topic_name = "topic1"
kafka_bootstrap_servers = 'localhost:9092'


spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")