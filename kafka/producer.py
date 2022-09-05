from email import message
from time import sleep
from kafka_init import producer
import pandas as pd

df = pd.read_csv(
    r"/home/selcuk/spark_proje/streaming_project/data/user_data_set_1.csv")
columns = df.columns


i = 0
message = {}
while i < len(df):
    row = df.values[i]
    for key, value in zip(columns, row):
        message.update({key: value})
    print(message)
    print("*" * 50)
 #   producer.send("topic1", {"a": 1, "b": 2})
    producer.send("topic1", message)
    i += 1
    sleep(5)
