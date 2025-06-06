#!/usr/bin/env python3

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct

def main():
   spark = SparkSession.builder.appName("dataproc-kafka-read-stream-app").getOrCreate()

   query = spark.readStream.format("kafka")\
      .option("kafka.bootstrap.servers", "rc1a-b90khsqmvlbkas2h.mdb.yandexcloud.net:9091") \
      .option("subscribe", "mh_changes") \
      .option("kafka.security.protocol", "SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=user1 "
              "password=password1 "
              ";") \
      .option("startingOffsets", "earliest")\
      .load()\
      .selectExpr("CAST(value AS STRING)")\
      .where(col("value").isNotNull())\
      .writeStream\
      .trigger(once=True)\
      .queryName("received_messages")\
      .format("memory")\
      .start()

   query.awaitTermination()

   df = spark.sql("select value from received_messages")

   df.write.format("text").save("s3a://ms-hw1/kafka-read-stream-output")

if __name__ == "__main__":
   main()
