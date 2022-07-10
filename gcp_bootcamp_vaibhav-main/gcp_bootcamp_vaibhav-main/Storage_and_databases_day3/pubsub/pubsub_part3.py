#Publish five messages to the “topic1” by incrementing “id” and changing the value of “name”.
#{  "id": 6,  "name": “xxx"}
#{  "id": 7,  "name": “yyy"}
#{  "id": 8,  "name": “ccc"}
#{  "id": 9,  "name": “zzz"}
#{  "id": 10,  "name": “bbb"}


"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1


# TODO(developer)
project_id = "vaibhav-gupta-bootcamp"
topic_id = "topic1"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

name=['xxx','yyy','ccc','zzz','bbb']
for n in range(6, 11):
    name1= (name[n-6])
    data = ("id : {},name : {}".format(n,name1))
    data = data.encode("utf-8")
    # Add two attributes, origin and username, to the message
    future = publisher.publish(
        topic_path, data, origin="python-sample", username="gcp"
    )
    print(future.result())

print(f"Published messages with custom attributes to {topic_path}.")
