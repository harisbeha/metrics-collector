#!/usr/bin/env python
import os
import time
import json
import logging
from datetime import datetime

import aiohttp
import asyncio
import socketio
import async_timeout

from sanic import Sanic
from sanic import response
import rabbitpy
from settings import rabbit_mon_settings

from google.cloud import monitoring
from google.oauth2 import service_account

RABBIT_USER = os.environ.get('RABBIT_USER', '')
RABBIT_PASS = os.environ.get('RABBIT_PASS', '')
RABBIT_HOST = os.environ.get('RABBIT_HOST', '')

app = Sanic()
sio = socketio.AsyncServer(async_mode='sanic')
sio.attach(app)

logger = logging.getLogger(__name__)

credentials = service_account.Credentials.from_service_account_file('serviceaccount.json')

# Instantiate Client
client = monitoring.Client(project='coresystem-171219', credentials=credentials)

resource = client.resource(
    'gke_container',
    labels={
        'project_id': 'coresystem-171219',
        'zone': 'us-central1-a',
        'cluster_name': 'core-cluster',
        'container_name': '',
        # Namespace and instance ID don't matter
        'namespace_id':  'prod-main',
        'instance_id': 'cloudamqp',
        'pod_id': 'cloudamqp',
    }
)

def create_queue_metric():
    metric_name = 'custom.googleapis.com/rabbit_iobaseline_combined_count'
    _metric = client.metric(type_=metric_name, labels=({}))
    return _metric

async def fetch(session, url, data=None, method='GET', r_type='json'):
    async with session.request(method, url, data=data) as response:
        if r_type == 'json':
            response = await response.json()
        elif r_type == 'text':
            response = await response.text()
        return response


async def http(url, *args, timeout=15, data=None, headers=None, method='GET', r_type='json'):
        uri = url

        if headers is None:
            headers = {}

        with async_timeout.timeout(timeout):
            async with aiohttp.ClientSession(headers=headers) as session:
                response = await fetch(session, uri, data=data, method=method)
                return response

# TODO Replace with Cielo queues and parse qs params, format string
# Alternatively pull overview base result, loop through items array, send metric for each, write total timeseries
async def run_check():
    query_url = 'https://{0}:{1}@{2}.rmq.cloudamqp.com/api/queues?page=1&page_size=100&name=(accounts|billing|finance|notification|integrations_mt|upwork|workmarket|merge_el_data|statcollectors|cv24)&use_regex=true&pagination=true'.format(RABBIT_USER, RABBIT_PASS, RABBIT_HOST)
    result = await app.http(url=query_url, r_type='json')
    items = result['items']
    aggregate_total = 0
    for item in items:
        ready_count = int(item['messages_ready'])
        unack_count = int(item['messages_unacknowledged'])
        total_count = ready_count + unack_count
        aggregate_total += total_count
    total_metric = create_queue_metric()
    client.write_point(total_metric, resource, aggregate_total)

async def ship_stats_dev(url, result):
    await app.http(url=url, data=json.dumps(result), method="POST", r_type="json")

async def check_rabbit():
    """
    Collect and send RabbitMQ queue counts to StackDriver
    Use session object to perform 'get' request on Kubernetes Event API
    Post data to endpoint or send to StackDriver client
    :return:
    """
    while True:
        await sio.sleep(10)
        await run_check()

@app.listener('before_server_start')
def before_server_start(sanic, loop):
    app.http = http
    sio.start_background_task(check_rabbit)

@app.route("/health")
async def health(request):
    return response.text("OK")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080, workers=1)
