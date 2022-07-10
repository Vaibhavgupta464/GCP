#Use your earlier python code to generate a CSV file “f100k.csv” with 100K rows and at least 50 columns on your Cloud Shell VM.

import logging
from random import randint
import google.cloud.logging
from datetime import datetime
import pandas as pd
import random
from faker import Faker

fake = Faker()
def create_rows(num=1):
    output = [{"id":randint(1,100000),
               "name":fake.name(),
               "bs":fake.bs(),
               "Order_Quantity": randint(1,20),
               "Regular_Customer":bool(random.getrandbits(1)),
               "city":fake.city(),
               "state":fake.state(),
               "PinCode": fake.zipcode(),
               "spend": randint(1,20),
               "randomdata":random.randint(1000,2000)} for x in range(num)]
    return output

df1 = pd.DataFrame(create_rows(100000))


def create_rows1(num=1):
    output = [{"id_1":randint(1,100000),
    "id2":randint(1,100000),
    "id3":randint(1,100000),
    "id4":randint(1,100000),
    "id5":randint(1,100000),
    "id6":randint(1,100000),
    "id7":randint(1,100000),
    "id8":randint(1,100000),
    "id9":randint(1,100000),
    "id10":randint(1,100000)
    }for x in range(num)]
    return output

df2 = pd.DataFrame(create_rows1(100000))

df3=df2.copy()
df3.columns=['id11','id12','id13','id14','id15','id16','id17','id18','id19','id20']

df4=df2.copy()
df4.columns=['id21','id22','id23','id24','id25','id26','id27','id28','id29','id30']

df5=df2.copy()
df5.columns=['id31','id32','id33','id34','id35','id36','id37','id38','id39','id40']

df_final = pd.concat([df1, df2,df3,df4,df5], axis=1)
print(df_final.shape)
df_final.to_csv(r"/home/fagcpdebc02_011/Dataflow/f100k.csv", index=False)
