#Create a DAG to upload Avro file data from GCS bucket to a BigQuery table. Dataset name “dag-dataset” and table name “composer_data”

from __future__ import print_function
import airflow
from airflow import models
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import python_operator
import datetime

default_dag_args = {
    'start_date': datetime.datetime(2022, 1, 13),
}

with models.DAG(
    'GCS-to-BQ',
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args) as dag:
    def move():
                from google.cloud import bigquery

                # TODO(developer): Replace with your values
                project = 'vaibhav-gupta-bootcamp'  # Your GCP Project
                location = 'US'  # the location where you want your BigQuery data to reside. For more info on possible locations see https://cloud.google.com/bigquery/docs/locations
                dataset_name = 'dag_dataset'

                from google.cloud import bigquery

                # Construct a BigQuery client object.
                client = bigquery.Client()
                table_id = "dag_dataset.composer_data"

                job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)
                uri = "gs://vaibhav-gupta-composer/users1.avro"

                load_job = client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )  # Make an API request.

                load_job.result()  # Waits for the job to complete.

                destination_table = client.get_table(table_id)
                print("Loaded {} rows.".format(destination_table.num_rows))

# priority_weight has type int in Airflow DB, uses the maximum.

t1 = python_operator.PythonOperator(
    task_id='composer',
    python_callable=move,
    dag=dag,depends_on_past=False,
    priority_weight=2**31-1)
