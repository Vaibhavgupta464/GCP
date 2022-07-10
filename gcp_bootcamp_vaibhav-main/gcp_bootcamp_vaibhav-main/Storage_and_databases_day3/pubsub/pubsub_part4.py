#Read messages from “topic1” using “sub2” subscription and write them into a file “msg_from_sub2.json”

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

import json

from google.cloud import pubsub_v1
import os
import logging
import google.cloud.logging
from google.cloud import bigquery
from json import JSONEncoder


# TODO(developer)
# project_id = "your-project-id"
# subscription_id = "your-subscription-id"
# Number of seconds the subscriber should listen for messages
# timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path("vaibhav-gupta-bootcamp", "sub2")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    with open('msg_from_sub2.json', 'a') as f:  
            f.write("Recieved " + str(message))
            print("Received " + str(message))
            #message.ack()

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=5)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.