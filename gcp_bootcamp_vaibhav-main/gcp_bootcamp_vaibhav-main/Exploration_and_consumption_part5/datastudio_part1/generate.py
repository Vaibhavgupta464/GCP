#Generate data using Python where possible. Generate data for 200 Customers, 50 Products, 5000 Orders, 50000 order_quantity. Save it in CSV or JSON
#Load data to Bigquerytables from above files using Python.


import logging
from random import randint
import google.cloud.logging
from datetime import datetime
import pandas as pd
import random
from faker import Faker
fake = Faker()

now = datetime.now()
timestamp = str(now.strftime("%Y%m%d_%H%M%S"))
fake = Faker()
def create_rows_customer_master(num=1):
    output = [{"customerid":randint(1,50),
               "name":fake.name(),
               "address":fake.bs(),
               "city":fake.city(),
               "state":fake.state(),
               "pinCode": fake.zipcode()} for x in range(num)]
    return output

def create_rows_order_details(num=1):
    output = [{"orderid":randint(101,120),
               "customerid":randint(1,50),
               "orderplaceddatetime":fake.date_time(),
               "Ordercompletiondatetime":fake.date_time(),
               "orderstatus":random.choice(["inprogress","delivered","shipped"])} for x in range(num)]
    return output

def create_rows_order_quantity(num=1):
    output = [{"orderid":randint(101,120),
               "productid":randint(11,20),
               "quantity":randint(1,100000)} for x in range(num)]
    return output

def create_rows_product_master(num=1):
    output = [{"productid":randint(11,20),
               "productcode":randint(1,20),
               "productname":fake.name(),
               "sku":randint(1,20),
               "rate":randint(100,120),
               "isactive": random.choice(["True","False"])} for x in range(num)]
    return output

df_customer_master = pd.DataFrame(create_rows_customer_master(201))
df_customer_master.to_csv(r"/home/fagcpdebc02_011/datastudio/customer_master.csv", index=False)

df_order_details = pd.DataFrame(create_rows_order_details(501))
df_order_details.to_csv(r"/home/fagcpdebc02_011/datastudio/order_details.csv", index=False)

df_order_quantity = pd.DataFrame(create_rows_order_quantity(50001))
df_order_quantity.to_csv(r"/home/fagcpdebc02_011/datastudio/order_quantity.csv", index=False)

df_product_master = pd.DataFrame(create_rows_product_master(51))
df_product_master.to_csv(r"/home/fagcpdebc02_011/datastudio/product_master.csv", index=False)

#------------------------------

from google.cloud import bigquery
client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
autodetect = True,
skip_leading_rows=1,
# The source format defaults to CSV, so the line below is optional.
source_format=bigquery.SourceFormat.CSV,
)


with open(r"/home/fagcpdebc02_011/datastudio/customer_master.csv", "r") as source_file:
    table_id = "vaibhav-gupta-bootcamp.mydukan.customer_master"
    load_job = client.load_table_from_dataframe(df_customer_master,table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id) # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

with open(r"/home/fagcpdebc02_011/datastudio/order_details.csv", "r") as source_file:
    table_id = "vaibhav-gupta-bootcamp.mydukan.order_details"
    load_job = client.load_table_from_dataframe(df_order_details,table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id) # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

with open(r"/home/fagcpdebc02_011/datastudio/order_quantity.csv", "r") as source_file:
    table_id = "vaibhav-gupta-bootcamp.mydukan.order_quantity"
    load_job = client.load_table_from_dataframe(df_order_quantity,table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id) # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

with open(r"/home/fagcpdebc02_011/datastudio/product_master.csv", "r") as source_file:
    table_id = "vaibhav-gupta-bootcamp.mydukan.product_master"
    load_job = client.load_table_from_dataframe(df_product_master,table_id, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(table_id) # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
