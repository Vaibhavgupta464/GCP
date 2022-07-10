#use the below code to log the python code in GCP logging.

import logging
import google.cloud.logging
from google.cloud import bigquery

def use_logging_handler():
    clientlogging = google.cloud.logging.Client()
    clientlogging.setup_logging()
    text = "Completed"
    logging.info(text)
    print("Logged: {}".format(text))

def msg():
    print("message")

if __name__ == "__main__":
    use_logging_handler()
    
msg()
