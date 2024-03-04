# RestApi Python/SQL Database + Docker flask/spark container

Endpoint to load batch 1 up to 1000 transactions (HTTP Post Request)

* Load dimension data (departments & jobs)
`http://127.0.0.1:5000/load/dims`

* Load hired employees data 
`http://127.0.0.1:5000/load/hired_employees`



## To call avro backup feature: Read package from spark/avro version

    `PUT /backupAvro`

        os.system("/opt/homebrew/Cellar/apache-spark/3.5.1/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.1 compressAvro.py &")

