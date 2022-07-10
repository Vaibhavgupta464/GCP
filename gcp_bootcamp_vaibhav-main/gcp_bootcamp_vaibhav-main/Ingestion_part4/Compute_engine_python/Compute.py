#To read all 4 tables from database “myorg” in Cloud SQL Instance “mypginstance” and create 
#tables in Bigquery dataset “pgdataset” with data from those tables. Plan for incremental data 
#load as well i.e. your program should be able to pick up new data when you run in next time.



import datetime
import os
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import pytz
import psycopg2
from configparser import ConfigParser

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=r'/home/fagcpdebc02_011/bigquery.json'
def query(q):
    conn = psycopg2.connect(host="34.136.78.92", database="postgres",port="5432", user="postgres", password="aLmktBD4GnGgkb97")
    return pd.read_sql(q, conn)

client = bigquery.Client(project="vaibhav-gupta-bootcamp")
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
dataset="pgdataset"

table=['employee','project','project_staff','deptarment', 'dept_summary','project_staff_count'
]
# array for list of tables to be created and input data
for n in table:
    print(n)
    st='SELECT * FROM ' + n
    df = query (st)
    df2=pd.DataFrame(df)
    table_id="vaibhav-gupta-bootcamp.{}.{}".format(dataset,n)
    
    table = bigquery.Table(table_id)
    job = pandas_gbq.to_gbq(df2, table_id)
    print("Loaded {}".format( table_id ))

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)


    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db
