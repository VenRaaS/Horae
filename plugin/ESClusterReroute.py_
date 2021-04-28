import logging
import json
import requests
import datetime

from lib.event import EnumEvent
from lib.subscr import EnumSubscript
import plugin.Task as Task


logger = logging.getLogger(__file__)


class ESClusterReroute(Task.Task):
    INVOKE_INTERVAL_SEC = 60
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_es-cluster'] ]
    LISTEN_EVENTS = [ EnumEvent['CRON_SCHEDULER'] ]
    PUB_TOPIC = None

    #-- ES APIs
    LB_ES_HOSTS = 'es-node-01'
    URL_CLUSTER = 'http://{h}:9200/_cluster/'
    URL_CLUSTER_HEALTH = URL_CLUSTER.format(h=LB_ES_HOSTS) + 'health?level=indices'
    #  https://www.elastic.co/guide/en/elasticsearch/reference/5.6/cluster-reroute.html
    URL_CLUSTER_RETRY_FAILED =  URL_CLUSTER.format(h=LB_ES_HOSTS) + 'reroute?retry_failed'

    INDEX_CATS_2_RESERVED_DAYS = {
        'oua': 14,
        'opp': 14, 
        'gocc': 3,
        'mod': 3,
    }


    def exe(self, hmsg) :
        if hmsg.get_attributes():
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if EnumEvent[event_type] in ESClusterReroute.LISTEN_EVENTS:
                objectIds = hmsg.get_objectIds()
                codename = hmsg.get_codename()

                url = ESClusterReroute.URL_CLUSTER_HEALTH
                resp = requests.get(url)
                healthJson = json.loads(resp.text)
                logger.info('[{0}] with status [{1}]'.format(healthJson['cluster_name'], healthJson['status'] ))
                if 'green' == healthJson['status']:
                    logger.info('great status nothing to do.')
                    return

                for k, v in healthJson['indices'].iteritems():
                    if 0 < v['unassigned_shards']:
                        logger.warn('[{0}] has {1} unassigned_shards'.format(k, v['unassigned_shards']))
                        url = ESClusterReroute.URL_CLUSTER_RETRY_FAILED
                        resp = requests.post(url)
                        logger.info('{0}, {1}'.format(url, json.loads(resp.text)['acknowledged']))
                        break
        


