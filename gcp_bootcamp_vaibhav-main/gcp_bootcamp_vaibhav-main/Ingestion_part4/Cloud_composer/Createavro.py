#Create an Avro file with 20K records and upload it on GCS bucket “firstname-lastname-composer” using python program. Use Service Accounts only

import avro.schema
import random
import string
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from google.cloud import bigquery, storage
from oauth2client.service_account import ServiceAccountCredentials
import os
import logging
import google.cloud.logging

def use_logging_handler():
    clientlogging = google.cloud.logging.Client()
    clientlogging.setup_logging()
    text = "Completed"
    logging.info(text)
    print("Logged: {}".format(text))
if __name__ == "__main__":
    use_logging_handler()
    
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fagcpdebc02_011/gcs.json"

schema = avro.schema.parse(open("user.avsc").read())
writer = DataFileWriter(open("users1.avro", "wb"), DatumWriter(), schema)

for i in range(0,20000):
    writer.append({"name":''.join(random.choices(string.ascii_uppercase + string.digits, k = 7)), "age": random.randint(0,22)})

writer.close()
reader = DataFileReader(open("users1.avro", "rb"), DatumReader())
for user in reader:
    print(user)
    print('===================')
reader.close()

client = storage.Client(project='vaibhav-gupta-bootcamp')
bucket = client.get_bucket('vaibhav-gupta-composer')
blob = bucket.blob('users1.avro')
blob.upload_from_filename('/home/fagcpdebc02_011/users1.avro')
