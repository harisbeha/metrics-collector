#!/usr/bin/env python
import gevent.monkey

gevent.monkey.patch_all()
import gevent
from gevent import spawn
import sys
import time

SD_METRIC_CONFIG = {
    'format': 'custom.googleapis.com/{name}'
}

RABBIT_CONFIGS = {
    'prod-main': [
        {
            'name': 'rabbit_{queue_name}_count',
            'queues': ['hitmansync', 'autonomousactions', 'slowtasks', 'hitman', 'payment', 'integrations', 'actions',
                       'integrationsmt', 'workmarket', 'partners', 'mergeeldata', 'autocap', 'celery', 'kaltura',
                       'priorityautocap', 'workflowexecution', 'ftp', 'cv24', 'metrics', 'throttler'],
            'aggregation': None,
            'labels': {},
            'host': '1.2.3.4',
            'auth': {'username': '', 'password': ''},
        },
        {
            'aggregated_name': 'rabbit_iobaseline_combined_count',
            'queues': ['accounts', 'billing', 'finance', 'notification', 'integrations_mt', 'upwork', 'workmarket',
                       'merge_el_data', 'statcollectors', 'cv24'],
            'aggregation': 'sum',
            'labels': {},
            'host': '1.2.3.4',
            'auth': {'username': '', 'password': ''},
        },
        {
            'aggregated_name': 'rabbit_media_download_combined_total_count',
            'queues': ['media_download', 'priority_media_download', 'background_media_download'],
            'aggregation': 'sum',
            'labels': {},
            'host': '1.2.3.4',
            'auth': {'username': '', 'password': ''},
        }
    ]
}

AGGREGATION_CONF = {
    'sum': sum,
}




def make_handler():
    def rabbit_handler(queue_config):
        while True:

            queue_names = '|'.join(queue_config['queues'])
            auth_info = queue_config['auth']
            url = 'https://{user}:{password}@{host}.rmq.cloudamqp.com/api/queues?page=1&name=({queue_names})&use_regex=true&pagination=false'.format(
                queue_names=queue_names, user=auth_info['username'], password=auth_info['password'], host=queue_config['host'])
            # get values from rabbit with requests
            value = {'a': 1}

            agg_name = queue_config.get('aggregation', None)
            if agg_name:
                assert agg_name in AGGREGATION_CONF, agg_name
                value = AGGREGATION_CONF[agg_name](value)
                datums = [{'value': value, 'name': name}]
            else:
                datums = []
                for q in queue_names:
                    datums.append({'value': value[q], 'name': queue_config['name'].format(queue_name=q)})

            # publish datums to SD, pull labels

            time.sleep(31)
    return spawn(rabbit_handler)





rabbit_mon_configs = [entry for name, config in RABBIT_CONFIGS.iteritems() for entry in config]
monitors = [
    spawn(rabbit_monitor),

]

# If any exits, crash the process.
gevent.wait(monitors, count=1)
sys.exit('A monitor crashed.')
