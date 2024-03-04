import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Compress").master("local").getOrCreate()

#Departments
databackup_dep = pd.read_csv('data/backup/csv/departments.gz', index_col=0)
df_dept = spark.createDataFrame(databackup_dep)

# Save as Avro
df_dept.write.format('avro').mode('overwrite').save('data/backup/avro/departmentsAvro')

#Jobs
databackup_jobs = pd.read_csv('data/backup/csv/jobs.gz', index_col=0)
df_jobs = spark.createDataFrame(databackup_jobs)

# Save as Avro
df_jobs.write.format('avro').mode('overwrite').save('data/backup/avro/jobsAvro')

#Hired Employees
databackup_empl = pd.read_csv('data/backup/csv/hired_employees.gz', index_col=0)
df_empl = spark.createDataFrame(databackup_empl)

# Save as Avro
df_empl.write.format('avro').mode('overwrite').save('data/backup/avro/hired_employeesAvro')

