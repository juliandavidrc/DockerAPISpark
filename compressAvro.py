import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compress").master("local").getOrCreate()

databackup = pd.read_csv('data/backup/csv/departments.gz', index_col=0)
df = spark.createDataFrame(databackup)

# Save as Avro
df.write.format('avro').mode('overwrite').save('data/backup/avro/departmentsAvro')

