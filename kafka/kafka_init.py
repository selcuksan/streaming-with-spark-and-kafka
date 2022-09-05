from kafka import KafkaProducer
from json import dumps

KAFKA_TOPIC_NAME_CONS = "topic1"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS_CONS],
    value_serializer=lambda x: dumps(x).encode("utf-8")
)
