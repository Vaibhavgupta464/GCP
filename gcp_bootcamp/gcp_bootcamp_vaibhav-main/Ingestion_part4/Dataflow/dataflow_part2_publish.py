#Use your python program from PubSub–Part 3 to publish Json messages to this topic.

"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
import os
import json

# TODO(developer)
project_id = "vaibhav-gupta-bootcamp"
topic_id = "dftopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

while(True):
    name=input("Enter name:")
    id =input("Enter id:")
    data = {"id":id,"name":name}
    # When you publish a message, the client returns a future.
    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    print(future.result())
    print("Published messages to {}.".format(topic_path))
