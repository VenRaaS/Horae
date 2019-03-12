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
import redis
from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
import lib.utility as utility
import plugin.Task as Task


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s', datefmt='%Y-%m-%d %I:%M:%S')
logger = logging.getLogger(__file__)

#-- redis-py, see https://github.com/andymccurdy/redis-py
HOST_RDS = 'ms-node-01'
PORT_RDS = '6379'
TIMEOUT_IN_SEC = 10
rds = redis.StrictRedis(host=HOST_RDS, port=6379, socket_connect_timeout=TIMEOUT_IN_SEC)



class MSAlterDateAliases(Task.Task):
    INVOKE_INTERVAL_SEC = 1800
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_ms-cluster'] ]
    LISTEN_EVENTS = [ EnumEvent['CRON_SCHEDULER'] ]
    PUB_TOPIC = None

    VALID_DIFF_RATIO = 0.9
    COUNT_ITERSTION_SIZE = 200
    KEY_ALIASES_DATE = '/venraas/aliases_date/code_name/{cn}'


    def exe(self, hmsg) :
        if hmsg.get_attributes():
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if EnumEvent[event_type] in MSAlterDateAliases.LISTEN_EVENTS:
                objectIds = hmsg.get_objectIds()
                codename = hmsg.get_codename()

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', codename, event_type, objectIds, generation)

                #-- get latest DATE from patterned keys
                date_latest = None
                randkeys = [ rds.randomkey() for i in range(1000) ]
                date_set = set()
                for k in randkeys:
                    #-- search DATE pattern from the prefix of the key, i.e. YYYYMMDD (%Y%m%d)
                    k = k.split('/')[1]
                    m = re.search(r'[12]\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])', k)
                    if m:
                        date = m.group(0)
                        date_set.add(date)
                sorted_dates = sorted(date_set, reverse=True)
                logger.info('keys patterned by DATE: {0}'.format(sorted_dates))
                if not sorted_dates or len(sorted_dates) <= 0:
                    logger.warn('none of key with DATE pattern.')
                    return

                date_latest = sorted_dates[0]
                logger.info('latest DATE of keys: {d}'.format(d=date_latest))
                
                key2cnt_latest = self.key2count_GroupByKeyPrefix(codename, date_latest)
                for k, v in sorted(key2cnt_latest.iteritems()):
                    logger.info('{key}: {cnt:,} '.format(key=k, cnt=v))
                 
                date_alias = self.getDateAlias(codename)
                if not date_alias:
                    self.alter_dateAlias(codename, date_latest)
                else:
                    if date_alias != date_latest:
                        logger.info('current aliae DATE {da} != latest DATE {dl}'.format(da=date_alias, dl=date_latest))

                        areAllValidRatios = True
                        key2cnt_alias = self.key2count_GroupByKeyPrefix(codename, date_alias)
                        for k, v in sorted(key2cnt_alias.iteritems()):
                            k_latest = k.replace(date_alias, date_latest)
                            if k_latest in key2cnt_latest:
                                cnt_latest = key2cnt_latest[k_latest]
                                min_cnt = min(v, cnt_latest)
                                max_cnt = max(v, cnt_latest)
                                ratio = float(min_cnt) / float(max_cnt)

                                if MSAlterDateAliases.VALID_DIFF_RATIO <= ratio:
                                    logger.info('{key}: {cnt:,}, {vr} < {r:.3} is a valid ratio.'.format(key=k, cnt=v, vr=MSAlterDateAliases.VALID_DIFF_RATIO, r=ratio))
                                else:
                                    logger.warn('{mic}/{mxc}= {r:.2} < {vr} unable to alter DATE alias due to the invalid ratio of the key prefix [{k}].'.format( \
                                                mic=min_cnt, mxc=max_cnt, r=ratio, vr=MSAlterDateAliases.VALID_DIFF_RATIO, k=k_latest))
                                    areAllValidRatios = False
                                    break
                            else:
                                logger.warn('key: {k} is not founded.'.format(k=k_latest))
                                areAllValidRatios = False
                                break

                        if areAllValidRatios: 
                            self.alter_dateAlias(codename, date_latest)
                    else: 
                        logger.info('alias DATE {da} == latest DATE {dl}, nothing to do.'.format(da=date_alias, dl=date_latest))

                date_alias = self.getDateAlias(codename)

                #-- remove legacy DATE patterned keys
                logger.info('now Date alias: {d}'.format(d=date_alias))
                idx_alias = sorted_dates.index(date_alias)
                for i, d in enumerate(sorted_dates):
                    if d == date_alias: 
                        continue
                    # 1st is date alias
                    if 0 == idx_alias:
                        if i <= idx_alias + 1:
                            continue
                    else:
                        if i <= idx_alias:
                            continue

                    self.del_datePatternedKeys(codename, d)


    def alter_dateAlias(self, codename, date):
        k = MSAlterDateAliases.KEY_ALIASES_DATE.format(cn=codename)
        date_alias_obj = {'code_name': codename, 'aliases_date': date}
        v = json.dumps(date_alias_obj, separators=(',', ':'))
        rds.lpush(k, v)
        rds.ltrim(k, 0, 0)
        logger.info('date alias is altered as {d}'.format(d=date))


    def getDateAlias(self, code_name):
        dateAliases = rds.lrange(MSAlterDateAliases.KEY_ALIASES_DATE.format(cn=code_name), 0, 0)
        if not dateAliases or len(dateAliases) <= 0:
            return None

        dateAlias_dic = json.loads(dateAliases[0])
        date_alias = dateAlias_dic['aliases_date']
        return date_alias


    def key2count_GroupByKeyPrefix(self, code_name, date):
        # date forms as YYYYMMDD (%Y%m%d)
        key_pattern = '*{cn}_*_{d}/*'.format(cn=code_name, d=date)
        logger.info('counting patterned key: "{kp}"'.format(kp=key_pattern))

        key2cnt = {}
        for key in rds.scan_iter(key_pattern, MSAlterDateAliases.COUNT_ITERSTION_SIZE):
            k = key.split('/')
            k = '/'.join(k[:3])
            
            key2cnt[k] = key2cnt[k] + 1 if k in key2cnt else 1

        return key2cnt 


    def del_datePatternedKeys(self, code_name, date):
        # date forms as YYYYMMDD (%Y%m%d)
        key_pattern = '*{cn}_*_{d}/*'.format(cn=code_name, d=date)
        logger.info('scan and delete patterned key: "{kp}"'.format(kp=key_pattern))

        key2cnt = {}
        keys = []
        for key in rds.scan_iter(key_pattern, MSAlterDateAliases.COUNT_ITERSTION_SIZE):
            k = key.split('/')
            k = '/'.join(k[:3])
            
            key2cnt[k] = key2cnt[k] + 1 if k in key2cnt else 1

            keys.append(key)
            if MSAlterDateAliases.COUNT_ITERSTION_SIZE <= len(keys):
                rds.delete(*keys)
                keys = []
        rds.delete(*keys)
        logging.info('deleted keys which are prefixed as follows: {k2c}'.format(k2c=key2cnt))


if '__main__' == __name__:
   aa =  MSAlterDateAliases()
   aa.exe()


