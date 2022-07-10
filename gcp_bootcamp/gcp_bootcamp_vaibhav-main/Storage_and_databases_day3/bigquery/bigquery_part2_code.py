import google.cloud.logging
import logging
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fagcpdebc02_011/bigquery.json"

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

# Set table_id to the ID of the table to create.
table_id = "vaibhav-gupta-bootcamp.bikeshare.hourly_summary_trips"
           

# defing the schema for bigquery table
schema = [ bigquery.SchemaField("bikeid", "STRING", mode="REQUIRED"),
           bigquery.SchemaField("trip_date", "DATE", mode="REQUIRED"),
           bigquery.SchemaField("trip_Start_hour", "time", mode="REQUIRED"),
           bigquery.SchemaField("start_station_name", "STRING", mode="REQUIRED"),
           bigquery.SchemaField("trip_count", "INTEGER", mode="REQUIRED"),
           bigquery.SchemaField("total_trip_duration_minutes", "INTEGER", mode="REQUIRED"),
                ]

table = bigquery.Table(table_id, schema=schema)
table.time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,field="trip_date")
table.clustering_fields=["start_station_name"]

table = client.create_table(table) 
print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
     )
job_config = bigquery.QueryJobConfig(destination=table_id)

sql = """
        SELECT bikeid,extract (date from start_time) as trip_date,extract(time from start_time) as trip_start_hour ,
        start_station_name ,duration_minutes as total_trip_duration_minutes,
        count(bikeid) OVER(PARTITION BY bikeid ORDER BY bikeid asc ) trip_count
        FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` where bikeid is not null;
        """



# Start the query, passing in the extra configuration.
query_job = client.query(sql, job_config=job_config) # Make an API request.
query_job.result() # Wait for the job to complete.
print("Query results loaded to the table {}".format(table_id))





#-------------------------------------------------

#for creating a view

import google.cloud.logging
import logging
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/fagcpdebc02_011/bigquery.json"

def use_logging_handler():
    clientlogging = google.cloud.logging.Client()
    clientlogging.setup_logging()
    text = "Completed"
    logging.info(text)
    print("Logged: {}".format(text))
if __name__ == "__main__":
        use_logging_handler()
from google.cloud import bigquery

client = bigquery.Client()


view_id  = "vaibhav-gupta-bootcamp.bikeshare.busiest_stations_by_hour"
source_id = "vaibhav-gupta-bootcamp.bikeshare.hourly_summary_trips"

view = bigquery.Table(view_id)

view.view_query = f'SELECT start_station_name,extract(hour from trip_Start_hour) as trip_Start_hour,count(*) as count_trips FROM `{source_id}` group by start_station_name,trip_Start_hour order by count_trips desc'

# Make an API request to create the view

view = client.create_table(view)
print(f"Created {view.table_type}: {str(view.reference)}")

