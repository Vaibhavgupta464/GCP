#Load raw data to GCS bucket named “firstname-lastname-netflix” using Python program.
#Create a BigQuery dataset named “netflix”.

import google.cloud.logging
import logging
from google.cloud import bigquery, storage
import os
from google.oauth2 import service_account

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fagcpdebc02_011/bigquery.json"

#storage_credentials = service_account.Credentials.from_service_account_file('gcs.json')
storage_client=storage.Client(project = 'vaibhav-gupta-bootcamp',credentials =storage_credentials)
des_bucket=storage_client.bucket("vaibhav-gupta-netflix")
blob= des_bucket.blob("netflix.csv")

source= '/home/fagcpdebc02_011/netflix.csv'
blob.upload_from_filename(source)

#-------------------------------------------

def use_logging_handler():
     clientlogging = google.cloud.logging.Client()
     clientlogging.setup_logging()
     text = "Completed"
     logging.info(text)
     print("Logged: {}".format(text))

if __name__ == "__main__":
    use_logging_handler()

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.netflix".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30) # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

table_id = "{}.{}.netflix-raw-data".format(client.project, dataset.dataset_id)


schema = [
bigquery.SchemaField("Title", "STRING", mode="REQUIRED"),
bigquery.SchemaField("Genre", "STRING"),
bigquery.SchemaField("Tags", "STRING"),
bigquery.SchemaField("Languages", "STRING"),
bigquery.SchemaField("Series_or_Movie", "STRING"),
bigquery.SchemaField("Hidden_Gem_Score", "FLOAT"),
bigquery.SchemaField("Country_Availability", "STRING"),
bigquery.SchemaField("Runtime", "STRING"),
bigquery.SchemaField("Director", "STRING"),
bigquery.SchemaField("Writer", "STRING"),
bigquery.SchemaField("Actors", "STRING"),
bigquery.SchemaField("View_Rating", "STRING"),
bigquery.SchemaField("IMDb_Score", "FLOAT"),
bigquery.SchemaField("Rotten_Tomatoes_Score", "FLOAT"),
bigquery.SchemaField("Metacritic_Score", "FLOAT"),
bigquery.SchemaField("Awards_Received", "FLOAT"),
bigquery.SchemaField("Awards_Nominated_For", "FLOAT"),
bigquery.SchemaField("Boxoffice", "INTEGER"),
bigquery.SchemaField("Release_Date", "DATE"),
bigquery.SchemaField("Netflix_Release_Date", "DATE"),
bigquery.SchemaField("Production_House", "STRING"),
bigquery.SchemaField("Netflix_Link", "STRING"),
bigquery.SchemaField("IMDb_Link", "STRING"),
bigquery.SchemaField("Summary", "STRING"),
bigquery.SchemaField("IMDb_Votes", "INTEGER"),
bigquery.SchemaField("Image", "STRING"),
bigquery.SchemaField("Poster", "STRING"),
bigquery.SchemaField("TMDb_Trailer", "STRING"),
bigquery.SchemaField("Trailer_Site", "STRING")
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table) # Make an API request.
print(
"Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)


