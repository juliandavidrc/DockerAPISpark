import os
from dotenv import load_dotenv
from flask import Flask
import pandas as pd
import psycopg2


app = Flask(__name__)

load_dotenv()

app = Flask(__name__)
url = os.getenv("DATABASE_URL")
connection = psycopg2.connect(url)


path_dept = './data/departments.csv'
path_jobs = './data/jobs.csv'
path_hiredemp = './data/hired_employees.csv'

@app.post("/load/dims")
def query_dept():  
          
    #load df Deparment
    data_dept = pd.read_csv(path_dept, header=None, names=['id', 'tdepartment'])    

    #load df Jobs
    data_jobs = pd.read_csv(path_jobs, header=None, names=['id', 'job'])      

    # Queries Deparment
    ddl_table_dept = "CREATE TABLE IF NOT EXISTS departments(id int, department varchar(200));"
    dml_delete_dept = "TRUNCATE TABLE departments;"
    dml_insert_dept = "INSERT INTO departments (id,department) VALUES (%s,%s);"

    # Queries Jobs
    ddl_table_jobs = "CREATE TABLE IF NOT EXISTS jobs(id int, job varchar(200));"
    dml_delete_jobs = "TRUNCATE TABLE jobs;"
    dml_insert_jobs = "INSERT INTO jobs (id,job) VALUES (%s,%s);"    

    ## INSERT
    with connection:
        with connection.cursor() as cursor:

            #Load Deparment Table
            cursor.execute(ddl_table_dept)
            cursor.execute(dml_delete_dept) 
            #cursor.execute("select count(*) from departments")              
            for index, row in data_dept.iloc[0:1000,].iterrows():
                cursor.execute(dml_insert_dept, (row.id, row.tdepartment),)            

            #Load Deparment Jobs
            cursor.execute(ddl_table_jobs)  
            cursor.execute(dml_delete_jobs) 
            #cursor.execute("select count(*) from jobs")              
            for index, row in data_jobs.iloc[0:1000,].iterrows():
                cursor.execute(dml_insert_jobs, (row.id, row.job),)              
        
        connection.commit()
        
    return "Data Dims Loaded", 201


@app.route('/')
def run():
    return 'Hello, World App!'

if __name__ == '__main__':
   app.run()