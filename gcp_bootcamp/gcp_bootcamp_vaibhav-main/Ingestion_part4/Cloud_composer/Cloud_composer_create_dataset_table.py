#Create a DAG to upload Avro file data from GCS bucket to a BigQuery table. Dataset name “dag-dataset” and table name “composer_data”

# This script is a helper function to the Dataflow Template Operator Tutorial.
# It helps the user set up a BigQuery dataset and table that is needed
# for the tutorial.

# [START composer_dataflow_dataset_table_creation]

# Make sure to follow the quickstart setup instructions beforehand.
# See instructions here:
# https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries

# Before running the sample, be sure to install the bigquery library
# in your local environment by running pip install google.cloud.bigquery

from google.cloud import bigquery

# TODO(developer): Replace with your values
project = 'vaibhav-gupta-bootcamp'  # Your GCP Project
location = 'US'  # the location where you want your BigQuery data to reside. For more info on possible locations see https://cloud.google.com/bigquery/docs/locations
dataset_name = 'dag-dataset'

def create_dataset_and_table(project, location, dataset_name):
    # Construct a BigQuery client object.
    client = bigquery.Client(project)

    dataset_id = f"{project}.{dataset_name}"

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # Set the location to your desired location for the dataset.
    # For more information, see this link:
    # https://cloud.google.com/bigquery/docs/locations
    dataset.location = location

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # Make an API request.

    print(f"Created dataset {client.project}.{dataset.dataset_id}")

    # Create a table from this dataset.

    table_id = f"{client.project}.{dataset_name}.composer_data"

    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    # [END composer_dataflow_dataset_table_creation]
    return dataset, table

create_dataset_and_table(project, location, "dag_dataset")
