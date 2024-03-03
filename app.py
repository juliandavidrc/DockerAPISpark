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

@app.post("/load/hired_employees")
def query_hiredemp():               

    #load df Hired Employees
    data_hiredemp = pd.read_csv(path_hiredemp, header=None, names=['id', 'nameemp', 'field2', 'department_id', 'job_id'])       
    data_hiredemp=data_hiredemp.fillna(0) 
    data_hiredemp['field2'] = data_hiredemp['field2'].replace(0,'1900-01-01T00:00:00Z')   

    # Queries Hired Employees
    ddl_table_hired_emp = "CREATE TABLE IF NOT EXISTS hired_employees(id int, nameemp varchar(200), field2 varchar(100), department_id int, job_id int);"
    dml_delete_hired_emp = "TRUNCATE TABLE hired_employees;"
    dml_insert_hired_emp = "INSERT INTO hired_employees (id, nameemp, field2, department_id, job_id) VALUES (%s,%s,%s,%s,%s);"

    ## INSERT
    with connection:
        with connection.cursor() as cursor:
            #Load Hired Emp
            cursor.execute(ddl_table_hired_emp)
            cursor.execute(dml_delete_hired_emp) 
            #cursor.execute("select count(*) from hired_employees")              
            #for index, row in data_hiredemp.iloc[0:1000,].iterrows():
            for index, row in data_hiredemp.iterrows():
                cursor.execute(dml_insert_hired_emp, (row.id, row.nameemp, row.field2, row.department_id, row.job_id),)
          
        connection.commit()

@app.route('/')
def run():
    return 'Hello, World App!'

if __name__ == '__main__':
   app.run()