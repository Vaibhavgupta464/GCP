#Generate 10 orders and Order Quantity ( related to orders) files upload it to GCS. Make sure these files have customer and products from your master data.


import logging
from random import randint
import google.cloud.logging
from datetime import datetime
import pandas as pd
import random
from faker import Faker
fake = Faker()
from google.cloud import storage
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcs.json'

def create_rows_order_details(num=1):
    output = [{"orderid":randint(101,110),
               "customerid":randint(1,50),
               "orderplaceddatetime":fake.date_time(),
               "Ordercompletiondatetime":fake.date_time(),
               "orderstatus":random.choice(["inprogress","delivered","shipped"])} for x in range(num)]
    return output

def create_rows_order_quantity(num=1):
    output = [{"orderid":randint(101,110),
               "productid":randint(11,20),
               "quantity":randint(1,100000)} for x in range(num)]
    return output

df_order_details = pd.DataFrame(create_rows_order_details(10))

df_order_quantity = pd.DataFrame(create_rows_order_quantity(10))

print(df_order_details)
    
destination = f'gs://mydukan_order_details/order_details.csv'
df_order_details.to_csv(destination, index=False)

destination = f'gs://mydukan_order_details/order_quantity.csv'
df_order_quantity.to_csv(destination, index=False)
