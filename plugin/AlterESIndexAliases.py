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

    URL_ES_ROOT = 'http://es-node-01:9200/{idx}'

    URL_ES_GOCC_COUNT = 'http://es-node-01:9200/{cn}_gocc_{dt}/_count'

    #-- retrieving existing aliases
    #   see https://www.elastic.co/guide/en/elasticsearch/reference/1.7/indices-aliases.html#alias-retrieving
    URL_ES_GOCC_ALIAS = 'http://es-node-01:9200/_alias/{cn}_gocc'

    #-- add and remove aliases 
    #   see https://www.elastic.co/guide/en/elasticsearch/reference/1.7/indices-aliases.html#indices-aliases 
    URL_ES_ALIASES = 'http://es-node-01:9200/_aliases'


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
                    if '_gocc_' in esIdx:
                        m = re.search(r'_(\d{8})$', esIdx)
                        date_str = m.group(1)
                        date = datetime.strptime(date_str, '%Y%m%d')
                        yest = date - timedelta(days=1)
                        yest_str = yest.strftime('%Y%m%d')
                        logger.info('{} -> {}'.format(yest_str, date_str))
 
                        url_gocc_cnt_date = AlterESIndexAliases.URL_ES_GOCC_COUNT.format(cn=codename, dt=date_str)
                        url_gocc_cnt_yest = AlterESIndexAliases.URL_ES_GOCC_COUNT.format(cn=codename, dt=yest_str)
                        logger.info('{} -> {}'.format(url_gocc_cnt_yest, url_gocc_cnt_date))

                        cnt_date = utility.count_index_es(url_gocc_cnt_date)
                        cnt_yest = utility.count_index_es(url_gocc_cnt_yest)
                        cnt_ratio = float(cnt_date)/float(cnt_yest) if cnt_yest else 1.0
                        logger.info('count ratio (today/yesterday): {} / {} = {:.3f}'.format(cnt_date, cnt_yest, cnt_ratio))

                        if cnt_date <= 0:
                            logger.error('zero entity on {}'.format(url_gocc_cnt_date))
                            continue

                        if 0 == cnt_yest:
                            logger.warning('zero entity on {}, an new company or sync failed before.'.format(url_gocc_cnt_yest))

                        if 0.7 <= cnt_ratio:
                            idx_gocc = '{cn}_gocc_{dt}'.format(cn=codename, dt=date_str)
                            alias_gocc = '{cn}_gocc'.format(cn=codename)

                            rm_queries = []
                            url_query_alias = AlterESIndexAliases.URL_ES_GOCC_ALIAS.format(cn=codename)
                            r = requests.get(url_query_alias)
                            idx_old_dict = json.loads(r.text)
                            if 200 == getattr(r, 'status_code') and len(idx_old_dict.keys()):
                                #-- collect all indices with the identical alias
                                for k in idx_old_dict.keys():
                                    idx_gocc_old = k
                                    rm_queries.append( {'remove' : { 'index': idx_gocc_old, 'alias': alias_gocc}} )
                            
                            act_query = {'actions': [{'add': {'index': idx_gocc, 'alias': alias_gocc}}]}
                            if rm_queries:
                                act_query['actions'].extend(rm_queries)
                            logger.info(act_query)

                            resp = requests.post(AlterESIndexAliases.URL_ES_ALIASES, data=json.dumps(act_query))
                            logger.info(resp.text)
                        else:
                            logger.error('unreasonable ratio, please check raw GOCC count')

                    elif '_opp_' in esIdx:
                        idx_opp = esIdx
                        alias_opp = '{cn}_opp'.format(cn=codename)

                        url_idx = AlterESIndexAliases.URL_ES_ROOT.format(idx=idx_opp)
                        if utility.exists_index_es(url_idx):
                            act_query = {'actions': [{'add': {'index': idx_opp, 'alias': alias_opp}}]}
                            logger.info(act_query)

                            resp = requests.post(AlterESIndexAliases.URL_ES_ALIASES, data=json.dumps(act_query))
                            logger.info(resp.text)
                        else:
                            logger.info('{} is not existence'.format(url_idx))




