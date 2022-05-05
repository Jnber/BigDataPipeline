from kafka import KafkaConsumer
import threading
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext

from IPython.display import display
import pandas as pd
from pyspark.shell import sqlContext, sc
from pyspark.sql import SparkSession

consumer = KafkaConsumer(
    'crypto',
    bootstrap_servers=['hadoop-master:9092'])

from pyspark.sql import SparkSession

appName = "Kafka Examples"
master = "local"
data_source_format = 'org.apache.hadoop.hbase.spark'

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

kafka_servers = "hadoop-master:9092"

catalog = {
    "table": {"namespace": "default", "name": "cryptoInfo"},
    "rowkey": "key",
    "columns": {
        "col0": {"cf": "rowkey", "col": "id", "type": "string"},
        "col1": {"cf": "cf1", "col": "price", "type": "double"},
        "col2": {"cf": "cf1", "col": "price_timestamp", "type": "date"},
    }
}







def startJob():
    threading.Timer(30.0, startJob).start()
    df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "crypto") \
        .load()

    df = df.withColumn('key_str', df['key'].cast('string').alias('key_str')).drop(
        'key').withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value')
    pandasDF = df.toPandas()
    data = pandasDF.iloc[-1, 6]
    json_object = json.loads(data)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(len(json_object))
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    store_list = []

    for item in json_object:
        store_details = {"id": None, "price": None, "price_timestamp": None}
        store_details['id'] = item['id']
        store_details['price'] = item['price']
        store_details['price_timestamp'] = item['price_timestamp']
        store_list.append(store_details)

    df.write.option("header", "true").csv("hdfs://tp1-bigdata_hadoop-master_1:8088/root/csv")
    df.show(100)


startJob()
