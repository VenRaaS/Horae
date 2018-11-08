import logging
import json
import requests
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %I:%M:%S')

LB_ES_HOSTS = 'es-node-01'

INDEX_CATS_2_RESERVED_DAYS = {
    'oua': 14,
    'opp': 14, 
    'gocc': 3,
    'mod': 3,
}

#-- ES APIs
URL_CLUSTER = 'http://{h}:9200/_cluster/'
URL_CLUSTER_HEALTH = URL_CLUSTER.format(h=LB_ES_HOSTS) + 'health?level=indices'
URL_CLUSTER_RETRY_FAILED =  URL_CLUSTER.format(h=LB_ES_HOSTS) + 'reroute?retry_failed'




if '__main__' == __name__:
    url = URL_CLUSTER_HEALTH
    resp = requests.get(url)
    healthJson = json.loads(resp.text)
    logging.info('[{0}] with status [{1}]'.format(healthJson['cluster_name'], healthJson['status'] ))

    for k, v in healthJson['indices'].iteritems():
        if 0 < v['unassigned_shards']:
            logging.warn('[{0}] has {1} unassigned_shards'.format(k, v['unassigned_shards']))
            url = URL_CLUSTER_RETRY_FAILED
            resp = requests.post(url)
            logging.info('{0}, {1}'.format(url, json.loads(resp.text)['acknowledged']))
            break
        


