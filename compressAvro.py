import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compress").master("local").getOrCreate()

# List
data = [{
    'col1': 'Category A',
    'col2': 100
}, {
    'col1': 'Category B',
    'col2': 200
}, {
    'col1': 'Category C',
    'col2': 300
}]

df = spark.createDataFrame(data)
# print(type(data))
# print("---------------")
# print(type(df))

#df.show()

# Save as Avro
df.write.format('avro').mode('overwrite').save('data/backup/avro/departmentsAvro')
#/opt/homebrew/Cellar/apache-spark/3.5.1/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1 compressAvro.py