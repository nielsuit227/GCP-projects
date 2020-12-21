import time, io
import numpy as np
from google.cloud import pubsub
from fastavro import parse_schema, schemaless_writer


def publishData(devices=10, sample=0):
    for i in range(devices):
        # time.sleep(np.random.randint(1, 1000) / 1000)
        # time.sleep(1)
        print('New sample for device %i' % i)
        message = {
            'id': i,
            'ts': int(time.time())}
        for i in range(50):
            message['sensorname' + str(i)] = np.random.rand()
        bytes_writer = io.BytesIO()
        schemaless_writer(bytes_writer, schema, message)
        publisher.publish(topic_url, bytes_writer.getvalue())


# parameters
fields = []
for i in range(50):
    fields.append({'name': 'sensorname' + str(i), 'type': 'float'})
DEVS = 5
PROJECT = 'archtrial'
TOPIC = 'dataTopic'
raw_schema = {
    'type': 'record',
    'namespace': 'AvroPubSubPrint',
    'name': 'Entity',
    'fields': fields
}

schema = parse_schema(raw_schema)

# GCP PubSub
publisher = pubsub.PublisherClient()
topic_url = 'projects/{project}/topics/{topic}'.format(project=PROJECT, topic=TOPIC)

# Scheduler
for i in range(30 * 60):
    publishData(devices=2000, sample=i)