#!/usr/bin/env python
import asyncio
import logging
from datetime import datetime

from google.cloud import monitoring
from google.oauth2 import service_account

from kubernetes import client, config, watch
import requests as rr

METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/instance/"
METADATA_FLAVOR = {'Metadata-Flavor': 'Google'}

def get_instance_id():
    return rr.get(METADATA_URL + 'id', headers=METADATA_FLAVOR).text

UNIQUE_INSTANCE_ID = get_instance_id()

logger = logging.getLogger('k8s_events')
logger.setLevel(logging.DEBUG)

config.load_incluster_config()
v1 = client.CoreV1Api()
v1ext = client.ExtensionsV1beta1Api()

# There has to be a better way than committing this thing to the repo
credentials = service_account.Credentials.from_service_account_file('serviceaccount.json')

# Instantiate StackDriver client
sd_client = monitoring.Client(project='coresystem-171219', credentials=credentials)

# Set metric resource type
resource = sd_client.resource(
    'gce_instance',
    labels={
        'zone': 'us-central1-a',
        'instance_id': unique_id
    }
)

# Create metric object
def create_oom_metric(event, kind, name):
    _metric = sd_client.metric(
        type_='custom.googleapis.com/oom-pod-count',
        labels={
            'event': event,
            'kind': kind,
            'name': name,
        })
    return _metric

# TODO Implement async/await on shipping metrics
async def pod_watcher():
    w = watch.Watch()
    for event in w.stream(v1.list_pod_for_all_namespaces):
        event_reason = event['object'].status.to_dict().get('reason', '')
        if event_reason == 'OOM Killed':
            print(event['type'], event['object'], event['object'].metadata.name)
            status_metric = create_oom_metric(event['type'], event['object'].kind, event['object'].metadata.name)
            # Use value of 1 indicating that ONE pod has been marked for termination / killed
            sd_client.write_point(status_metric, resource, 1)
        asyncio.sleep(0)

# Watch specific deployment updates and ship to StackDriver as custom_metric
async def deployment_watcher():
    w = watch.Watch()
    for event in w.stream(v1ext.list_deployment_for_all_namespaces):
        logger.info("Event: %s %s %s" % (event['type'], event['object'].kind, event['object'].metadata.name))
        await asyncio.sleep(0)

# Grab event loop object
ioloop = asyncio.get_event_loop()

# Create async task and run indefinitely
ioloop.create_task(pod_watcher())
ioloop.create_task(deployment_watcher())
ioloop.run_forever()
