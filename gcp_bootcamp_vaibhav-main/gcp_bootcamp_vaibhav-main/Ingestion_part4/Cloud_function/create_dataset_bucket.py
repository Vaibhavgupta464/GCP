#Create a separate bucket “firstname-lastname-functions” and BigQuery dataset “cfunc” for this activity using your previous code and correct service accounts.


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

client = storage.Client(project='vaibhav-gupta-bootcamp')
bucket_name = 'vaibhav-gupta-functions'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
new_bucket = storage_client.create_bucket(bucket, location="us")
print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )

from google.cloud import bigquery

project = 'vaibhav-gupta-bootcamp'  # Your GCP Project
location = 'US'  # the location where you want your BigQuery data to reside. For more info on possible locations see https://cloud.google.com/bigquery/docs/locations
dataset_name = 'cfunc'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fagcpdebc02_011/bigquery.json"

def create_dataset(project, location, dataset_name):
    
    client = bigquery.Client(project)
    dataset_id = f"{project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    dataset = client.create_dataset(dataset)  # Make an API request.
    print(f"Created dataset {client.project}.{dataset.dataset_id}")

    return dataset

create_dataset(project, location, "cfunc")
