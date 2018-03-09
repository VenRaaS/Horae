import csv
import io
import json
import os
import re
import sys
import subprocess
import logging
from datetime import datetime, timedelta

import requests

from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
import lib.utility as utility
import plugin.Task as Task


logger = logging.getLogger(__file__)


class AlterESIndexAliases(Task.Task):
    INVOKE_INTERVAL_SEC = 600
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_es-cluster'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]
#    PUB_TOPIC = EnumTopic['es-cluster']

    URL_ES_GOCC_COUNT = 'http://es-node-01:9200/{}_gocc_{}/_count'
    URL_ES_GOCC_ALIASES = 'http://es-node-01:9200/_aliases'
 

    def exe(self, hmsg) :
        if hmsg.get_attributes():
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if 'OBJECT_FINALIZE' == event_type:
                objectIds = hmsg.get_objectIds()
                codename = hmsg.get_codename()

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', codename, event_type, objectIds, generation)
                
                for o in objectIds:
                    esIdx = o
                    logger.info(exIdx)
                    
                    if '_gocc_' in esIdx:
                        m = re.search(r'_(\d{8})$', esIdx)
                        date_str = m.group(1)
                        date = datetime.strptime(date_str, '%Y%m%d')
                        yest = date - timedelta(days=1)
                        yest_str = yest.strftime('%Y%m%d')
 
                        url_gocc_date = this.URL_ES_GOCC_COUNT.format(codename, date_str) 
                        url_gocc_yest = this.URL_ES_GOCC_COUNT.format(codename, yest_str) 

                        cnt_date = utility.count_index_es(url_gocc_date)
                        cnt_yest = utility.count_index_es(url_gocc_yest)
                        cnt_ratio = cnt_date/cnt_yest;
                        logger.info('count ratio: {} / {} = {}'.format(cnt_date, cnt_yest, cnt_ratio))
                    
                        if cnt_date <= 0:
                            logger.error('zero entity on {}'.format(url_gocc_date))
                            continue

                        if 0.7 <= cnt_ratio:
                            idx_gocc = '{cn}_gocc_{dt}'.format(cn=codename, dt=date_str)
                            alias_gocc = '{cn}_gocc'.format(ct=codename)
                            
                            act_alias = {'actions': [{'add': {'index': idx_gocc, 'alias': alias_gocc}}]}
                            resp = requests.post(URL_ES_GOCC_ALIASES, data=json.dumps(act_alias))
                            logger.info(resp.text)
                        else:
                            logger.error(' ')


