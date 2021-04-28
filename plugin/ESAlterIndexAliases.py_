import csv
import io
import json
import os
import re
import sys
import subprocess
import logging
import time
import urllib
from datetime import datetime, timedelta

import requests

from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
import lib.utility as utility
import plugin.Task as Task


#logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s', datefmt='%Y-%m-%d %I:%M:%S')
logger = logging.getLogger(__file__)


class ESAlterIndexAliases(Task.Task):
    INVOKE_INTERVAL_SEC = 60
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_es-cluster'] ]
    LISTEN_EVENTS = [ EnumEvent['CRON_SCHEDULER'] ]
    PUB_TOPIC = None
    
    LB_ES_HOSTS = 'es-node-01'

    INDEX_CATS_2_RESERVED_DAYS = {
        'oua': 14,
        'opp': 14, 
        'gocc': 3,
        'mod': 3,
    }

    VALID_DIFF_RATIO = 0.95

    #-- ES APIs
    URL_HOSTPORT = 'http://{h}:9200'.format(h=LB_ES_HOSTS)
    URL_DELETE_INDICE = URL_HOSTPORT + '/{idx}'
    URL_COUNT_INDICE = 'http://{h}:9200/{idx}/_count'
    URL_GLOBALTP_CHECK = 'http://{h}:9200/{idx}/tp/_search?q=category_code:GlobalTP'
    URL_SEARCH = 'http://{h}:9200/{idx}/_search'

    URL_ALIASES = 'http://{h}:9200/_aliases'
    JSON_ADD_RM_ALIAS = {"actions":[{"add":{"index":"{newidx}","alias":"{ali}"}},{"remove":{"index":"{oldidx}","alias":"{ali}"}}]}
    JSON_ADD_ALIAS = {"actions":[{"add":{"index":"{idx}","alias":"{cn}_{cat}"}}]}
    JSON_ADD_ALIASES = {"actions":[{"add":{"index":"{cn}_{cat}_*","alias":"{cn}_{cat}"}}]}
    JSON_AGG_GROUP_BY_TYPE = {"aggs":{"group_by_type":{"terms":{"field":"_type","size":100}}},"size":0}


    def exe(self, hmsg) :
        if hmsg.get_attributes():
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if EnumEvent[event_type] in ESAlterIndexAliases.LISTEN_EVENTS:
                objectIds = hmsg.get_objectIds()
                codename = hmsg.get_codename()

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', codename, event_type, objectIds, generation)

                for cate in ESAlterIndexAliases.INDEX_CATS_2_RESERVED_DAYS.keys():
                    alias = '{cn}_{cat}'.format(cn=codename, cat=cate)

                    indices, iaInfoJson = self.get_sorted_indices(alias)
                    if len(indices) <= 0:
                        logger.warn('none of indices with prefix {0}'.format(alias))
                        continue

                    #-- the latest index
                    idx_latest = indices[0]

                    #-- find the latest Aliased index
                    #   iaInfoJson, e.g. {"$CN_gocc_20181107":{"aliases":{"$CN_gocc":{}}}, "$CN_gocc_20181105":{"aliases":{}}, ...}
                    alias_latest = next((i for i in indices if alias in iaInfoJson[i]['aliases']), None)

                    #-- alter aliases
                    if cate in ['oua', 'opp']:
                        if None == alias_latest or \
                           alias_latest != idx_latest:
                            ESAlterIndexAliases.JSON_ADD_ALIASES['actions'][0]['add']['index'] = '{cn}_{cat}_*'.format(cn=codename, cat=cate)
                            ESAlterIndexAliases.JSON_ADD_ALIASES['actions'][0]['add']['alias'] = '{cn}_{cat}'.format(cn=codename, cat=cate)
                            url = ESAlterIndexAliases.URL_ALIASES.format(h=ESAlterIndexAliases.LB_ES_HOSTS)
                            resp = requests.post(url, json=ESAlterIndexAliases.JSON_ADD_ALIASES)
                            logger.info('{0} --data {1}, {2}'.format(url, ESAlterIndexAliases.JSON_ADD_ALIASES, resp.text))
                        else:
                            logger.info('{0}(idx) = {1}(alias), latest indices are equal, awesome +1'.format(idx_latest, alias_latest))
                    elif cate in ['mod', 'gocc']:
                        if None == alias_latest:
                            # newborn, none alias yet
                            logger.info('newborn, none alias yet')
                            logger.info('let\'s alias the latest one {0} ...'.format(idx_latest))
                            ESAlterIndexAliases.JSON_ADD_ALIAS['actions'][0]['add']['index'] = idx_latest
                            ESAlterIndexAliases.JSON_ADD_ALIAS['actions'][0]['add']['alias'] = alias
                            url = ESAlterIndexAliases.URL_ALIASES.format(h=ESAlterIndexAliases.LB_ES_HOSTS)
                            resp = requests.post(url, json=ESAlterIndexAliases.JSON_ADD_ALIAS)
                            logger.info('{0} --data {1}, {2}'.format(url, ESAlterIndexAliases.JSON_ADD_ALIAS, resp.text))
                        elif alias_latest != idx_latest:
                            # the lastest alias != the latest index
                            logger.info('{0}(idx) <> {1}(alias), latest indices is not equal'.format(idx_latest, alias_latest))
                            logger.info('let\'s sync the alias to the latest index ...')

                            #-- check whether the number of docs is stable
                            cnt_set = set()
                            url = ESAlterIndexAliases.URL_COUNT_INDICE.format(h=ESAlterIndexAliases.LB_ES_HOSTS, idx=idx_latest)
                            for i in range(3):
                                resp = requests.get(url)
                                cnt_set.add( json.loads(resp.text)['count'] )
                                if 1 != len(cnt_set):
                                    logger.info('{0} is under syncing and skip alias-alter this time.'.format(url))
                                    return
                                time.sleep(10)
                            cnt_idx = float( cnt_set.pop() )
                            logger.info('{0} is stable with {1:,.0f} docs.'.format(url, cnt_idx))

                            if 'mod' == cate:
                                #-- check size of TP model
                                tp_cnts = {alias_latest:None, idx_latest:None}
                                for idx in [alias_latest, idx_latest]:
                                    url = ESAlterIndexAliases.URL_SEARCH.format(h=ESAlterIndexAliases.LB_ES_HOSTS, idx=idx)
                                    resp = requests.post(url, json=ESAlterIndexAliases.JSON_AGG_GROUP_BY_TYPE)
                                    resp_json = json.loads(resp.text)
                                    if 'aggregations' in resp_json:
                                        for b in resp_json['aggregations']['group_by_type']['buckets']:
                                            if 'tp' == b['key']:
                                                tp_cnts[idx] = float(b['doc_count'])
                                    else:
                                        logger.warn('unable to count types because {idx} is not found'.format(idx=idx))
                                        continue
                                
                                if all( tp_cnts.values() ):
                                    ratio = round(min(tp_cnts.values()) / max(tp_cnts.values()), 3)
                                    if ESAlterIndexAliases.VALID_DIFF_RATIO <= ratio:
                                        logger.info('{0} < {1}, TP ratio is valid.'.format(ESAlterIndexAliases.VALID_DIFF_RATIO, ratio))
                                    else:
                                        logger.warn('{0} < {1} unable to alter alias due to invalid TP ratio.'.format(ratio, ESAlterIndexAliases.VALID_DIFF_RATIO))
                                        continue
                                else:
                                    for k, v in tp_cnts.items():
                                        if v is None:
                                            msg = 'TP ratio validation error, index [{0}] is {1}'.format(k, v)
                                            logger.warn(msg)
                                            utility.warning2slack(codename, msg)
                                    continue
                            
                                #-- check availability of GlobalTP
                                url = ESAlterIndexAliases.URL_GLOBALTP_CHECK.format(h=ESAlterIndexAliases.LB_ES_HOSTS, idx=idx_latest)
                                resp = requests.get(url)
                                if int(json.loads(resp.text)['hits']['total']) < 1:
                                    msg = 'GlobalTP is unavailable in {0}, url: {1}'.format(idx_latest, url)
                                    logger.warn(msg)
                                    utility.warning2slack(codename, msg)
                                    continue
                                else:
                                    msg = 'GlobalTP is available in {0}, hits.total: {1}'.format(idx_latest, json.loads(resp.text)['hits']['total'])
                                    logger.info(msg)
                           
                            url = ESAlterIndexAliases.URL_COUNT_INDICE.format(h=ESAlterIndexAliases.LB_ES_HOSTS, idx=alias_latest)
                            resp = requests.get(url)
                            cnt_alias = float(json.loads(resp.text)['count'])
                            ratio = min(cnt_idx,cnt_alias) / max(cnt_idx,cnt_alias)
                            if ESAlterIndexAliases.VALID_DIFF_RATIO <= min(cnt_idx,cnt_alias) / max(cnt_idx,cnt_alias):
                                logger.info('{0} < {1}, valid ratio.'.format(ESAlterIndexAliases.VALID_DIFF_RATIO, ratio))
                                # json command
                                ESAlterIndexAliases.JSON_ADD_RM_ALIAS['actions'][0]['add']['index'] = idx_latest
                                ESAlterIndexAliases.JSON_ADD_RM_ALIAS['actions'][0]['add']['alias'] = alias 
                                ESAlterIndexAliases.JSON_ADD_RM_ALIAS['actions'][1]['remove']['index'] = alias_latest 
                                ESAlterIndexAliases.JSON_ADD_RM_ALIAS['actions'][1]['remove']['alias'] = alias
                                url = ESAlterIndexAliases.URL_ALIASES.format(h=ESAlterIndexAliases.LB_ES_HOSTS)
                                resp = requests.post(url, json=ESAlterIndexAliases.JSON_ADD_RM_ALIAS)
                                logger.info('{0} --data {1}, {2}'.format(url, ESAlterIndexAliases.JSON_ADD_RM_ALIAS, resp.text))
                            else:
                                logger.warn('{0}/{1}= {2} < 0.98 unable to alter alias [{3}] due to invalid ratio.'.format( \
                                    min(cnt_idx,cnt_alias), max(cnt_idx,cnt_alias), ratio, alias))
                        else:
                            logger.info('{0}(idx) = {1}(alias), latest indices are equal, awesome +1'.format(idx_latest, alias_latest))
                    
                    #-- purge outdated indices
                    indices, iaInfoJson = self.get_sorted_indices(alias)
                    if len(indices) <= 0:
                        logger.warn('none of indices with prefix {0}'.format(alias))
                        continue
                    
                    indices_aliasbeg = []
                    for i, idx in enumerate(indices):
                        if alias in iaInfoJson[idx]['aliases']:
                            indices_aliasbeg = indices[i: ]
                            break

                    revdays = ESAlterIndexAliases.INDEX_CATS_2_RESERVED_DAYS[cate]
                    if len(indices_aliasbeg) <= revdays:
                        logger.info('[{0}] has {1} indices and the latest alias on [{2}], <= {3}, which does not need to purge yet.'.format(alias, len(indices_aliasbeg), indices_aliasbeg[0], revdays))
                    else:
                        idx_end = revdays - len(indices_aliasbeg)
#                       ymd = idx_latest.split('_')[-1]
#                       dt_latest = datetime.datetime.strptime(ymd, '%Y%m%d')
#                       dt_end =  dt_latest - datetime.timedelta(days=revdays)
                        for idx in indices_aliasbeg[idx_end: ] :
                            urldel = ESAlterIndexAliases.URL_DELETE_INDICE.format(h=ESAlterIndexAliases.LB_ES_HOSTS, idx=idx)
                            resp = requests.delete(urldel)
                            logger.info('delete {0}, {1}'.format(urldel, resp.text))
                
                #-- purge unknown indices
                all_indices = self.list_all_indices(ESAlterIndexAliases.URL_HOSTPORT)                
                codenames = self.list_all_codenames(ESAlterIndexAliases.URL_HOSTPORT)
                if len(codenames) == 0 : 
                    return
                codenames.append('venraas')
                # indices whose prefix does not start with $codename or venraas
                unknownIndices = filter(lambda idx: not any([ idx.startswith(c) for c in codenames ]), all_indices)
                for idx in unknownIndices:                    
                    urldel = ESAlterIndexAliases.URL_DELETE_INDICE.format(idx=urllib.quote_plus(idx.encode('utf8')))
                    resp = requests.delete(urldel)
                    logger.info('delete unknown index: {0}, {1}'.format(urldel, resp.text))

    def get_sorted_indices(self, alias, sort_decending=True):
        indices = []
     
        resp = requests.get(ESAlterIndexAliases.URL_ALIASES.format(h=ESAlterIndexAliases.LB_ES_HOSTS))
        iaInfoJson = json.loads(resp.text)
        indices = filter(lambda k: alias in k, iaInfoJson.keys())

        if sort_decending:
            indices.sort(reverse=True)
        else:
            indices.sort()

        return (indices, iaInfoJson)

    def list_all_indices(self, h):
        rs =[]

        url = '{h}/_cat/indices?v'.format(h=h)
        try:
            resp = requests.get(url, timeout=2)
            text = resp.text.encode('utf8')
            csv_reader = csv.DictReader(text.split('\n'), delimiter=' ', skipinitialspace=True)
            rs = [ l['index'].decode('utf8') for l in csv_reader ]
        except Exception as e:
            logger.error(e, exc_info=True)
            
        return rs

    def list_all_codenames(self, h):
        rs =[]

        url = '{h}/venraas/com_pkgs/_search?sort=update_dt:desc&size=1'.format(h=h)
        try:
            resp = requests.get(url, timeout=2)
            coms_json = json.loads(resp.text)
            coms = coms_json['hits']['hits'][0]['_source']['companies']
            rs = [ c['code_name'] for c in coms ]
        except Exception as e:
            logger.error(e, exc_info=True)

        return rs




if '__main__' == __name__:
   aa =  ESAlterIndexAliases()
   aa.exe()

