#!/usr/bin/env python3

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, BooleanType

def main():
   spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

   schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("answer_date", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("self_employeed", BooleanType(), True),
        StructField("family_history", BooleanType(), True),
        StructField("treatment", BooleanType(), True),
        StructField("days_indoors", StringType(), True),
        StructField("growing_stress", StringType(), True),
        StructField("change_habits", StringType(), True),
        StructField("mental_health_history", StringType(), True),
        StructField("mood_swings", StringType(), True),
        StructField("coping_struggles", BooleanType(), True),
        StructField("work_interest", StringType(), True),
        StructField("social_weaknes", StringType(), True),
        StructField("mental_health_interview", StringType(), True),
        StructField("care_options", StringType(), True)])

   df = spark.read.csv('s3a://ms-hw1/2025/06/05/mental_health.csv', schema=schema)
   
   df = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias('value'))
   df.write.format("kafka") \
      .option("kafka.bootstrap.servers", "rc1a-b90khsqmvlbkas2h.mdb.yandexcloud.net:9091") \
      .option("topic", "mh_changes") \
      .option("kafka.security.protocol", "SASL_SSL") \
      .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
      .option("kafka.sasl.jaas.config",
              "org.apache.kafka.common.security.scram.ScramLoginModule required "
              "username=user1 "
              "password=password1 "
              ";") \
      .save()

if __name__ == "__main__":
   main()