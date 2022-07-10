#Load data from GCS to BigQuery table “netflix-raw-data”, identify right data types to represent each column into BigQuery and load data using Python.

from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
#pip3 install gcsfs
#pip3 install fsspec

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "vaibhav-gupta-bootcamp.netflix.netflix-raw-data"

uri = "gs://vaibhav-gupta-netflix/netflix.csv"

storage_client = storage.Client()
bucket = storage_client.get_bucket('vaibhav-gupta-netflix')
blob = bucket.blob('netflix.csv')
path = "gs://vaibhav-gupta-netflix/netflix.csv"

df = pd.read_csv(path)
df.columns = df.columns.str.replace(' ','_')
df['Release_Date'] = pd.to_datetime(df['Release_Date'],infer_datetime_format=True).dt.date
df['Boxoffice'] = df['Boxoffice'].str.replace('$','')



job_config = bigquery.LoadJobConfig(
autodetect = True,
skip_leading_rows=1,
# The source format defaults to CSV, so the line below is optional.
source_format=bigquery.SourceFormat.CSV,
)

table_id = "vaibhav-gupta-bootcamp.netflix.netflix-raw-data"
load_job = client.load_table_from_dataframe(df,table_id, job_config=job_config
)
load_job.result()
destination_table = client.get_table(table_id) # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))


