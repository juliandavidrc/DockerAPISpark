#avro-example.py
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Restore").master("local").getOrCreate()

# Read Avro
df2 = spark.read.format('avro').load('data/backup/avro/departmentsAvro')
df2.write.option("header",True).csv("data/backup/csv/departments_restored")