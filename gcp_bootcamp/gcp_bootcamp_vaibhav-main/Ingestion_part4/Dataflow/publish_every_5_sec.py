#Execute your python program to publish 5 messages every 5 seconds for 5 minutes.


"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
import os
import json
import time
time.sleep(5)

# TODO(developer)
project_id = "vaibhav-gupta-bootcamp"
topic_id = "dftopic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


name=["abc","sd","dgr","efe","fdf"]
id = ["1","2","3","4","5"]
for i,j in zip(id, name):
    data = {"id":i,"name":j}
    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
  
time.sleep(5)

name=["qoq","wd","fire","goat","laptop"]
id = ["6","7","8","9","10"]
for i,j in zip(id, name):
    data = {"id":i,"name":j}
    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))

time.sleep(5)

name=["heat","stark","bran","john","sansa"]
id = ["11","12","13","14","15"]
for i,j in zip(id, name):
    data = {"id":i,"name":j}
    future = publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
