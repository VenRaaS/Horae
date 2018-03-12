import csv
import io
import json
import os
import re
import sys
import subprocess
import datetime
import logging

import requests

from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
from lib.hmessage import HMessage
import lib.utility as utility
import plugin.Task as Task


logger = logging.getLogger(__file__)


class ImportGOCC2es(Task.Task):
    INVOKE_INTERVAL_SEC = 600
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bigquery'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]
    PUB_TOPIC = EnumTopic['es-cluster']

    SQL_EXPORT_UNIMA_GOODS = 'SELECT \'{}\' as code_name, SUBSTR(CAST(update_time AS STRING),0,19) AS update_time,  * EXCEPT (pgid, goods_describe, goods_spec, currency, provider, barcode_ean13, barcode_upc, first_rts_date, update_time) FROM {}'
    
    SQL_EXPORT_UNIMA_CATEGORY = 'SELECT \'{}\' as code_name, SUBSTR(CAST(update_time AS STRING),0,19) as update_time,  * EXCEPT (update_time) FROM {}'

    URL_ES_GOCC_COUNT = 'http://es-node-01:9200/{}_gocc_{}/_count'


    def exe(self, hmsg) :
        if hmsg.get_attributes():
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if 'OBJECT_FINALIZE' == event_type:
                objectIds = hmsg.get_objectIds()
                codename = hmsg.get_codename()

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', codename, event_type, objectIds, generation)

                folder = 'gocc2es'
                unpackPath = os.path.join('/tmp', folder)
                cmd = 'mkdir -p {}'.format(unpackPath)
                subprocess.call(cmd.split(' '))
                logger.info(cmd)

                #-- clean tmp local files
                legacy_globFNs = '{cn}_*'.format(cn=codename) 
                cmd = 'rm -f {}'.format(os.path.join(unpackPath, legacy_globFNs))
                subprocess.call(cmd.split(' '))
                logger.info(cmd)
                
                bucketId = 'ven-cust-{}'.format(codename)
                gsDataPath = os.path.join('gs://', bucketId, 'tmp', 'gocc2es')
                
                #-- clean tmp folder in GCS 
                cmd = 'gsutil rm -r -f {}'.format(gsDataPath)
                logger.info(cmd)
                subprocess.call(cmd.split(' '))

                gocc_date = None
                for t in objectIds:
                    srcDS, srcTb = t.split('.')

                    sql = ''
                    if srcTb.startswith('goods_'):
                        sql = ImportGOCC2es.SQL_EXPORT_UNIMA_GOODS.format(codename, t)
                        m = re.search(r'_(\d{8})$', srcTb)
                        if m:
                            gocc_date = m.group(1)
                    elif srcTb.startswith('category_'):
                        sql = ImportGOCC2es.SQL_EXPORT_UNIMA_CATEGORY.format(codename, t)
                    else:
                        continue

                    expoDS = '{}_tmp'.format(codename)
                    expoTb = 'gocc2es_{}'.format(srcTb)
                    expoDSTb = '{}.{}'.format(expoDS, expoTb)
                    cmd = 'bq query -n 0 --replace --use_legacy_sql=False --destination_table={} {}'.format(expoDSTb, sql)
                    logger.info(cmd)
                    subprocess.call(cmd.split(' '))

                    #-- export to GCS
                    srcFN = '{cn}_{tn}'.format(cn=codename, tn=srcTb)
                    gsJsonPath = os.path.join(gsDataPath, srcFN)
                    cmd = 'bq extract --destination_format=NEWLINE_DELIMITED_JSON \"{}\" {}'.format(expoDSTb, gsJsonPath)
                    logger.info(cmd)
                    subprocess.call(cmd, shell=True)

                    #-- download to local
                    cmd = 'gsutil cp {} {}'.format(gsJsonPath, unpackPath)
                    logger.info(cmd)
                    subprocess.call(cmd.split(' '))

                    #-- lowercase of json KEY, i.e. field name
                    lowerkeyFN = '{}.lk'.format(srcFN)
                    lowerkeyFP = os.path.join(unpackPath, lowerkeyFN)

                    with io.open(lowerkeyFP, 'w', encoding='utf-8') as fo:
                        rawFP = os.path.join(unpackPath, srcFN)
                        with io.open(rawFP, 'r', encoding='utf-8') as fi: 
                            for line in fi:
                                o = json.loads(line)
                                o_lowerkey = dict( (k.lower(), v) for k, v in o.iteritems() )
                                fo.write(json.dumps(o_lowerkey, ensure_ascii=False) + '\n')
                   
                    #-- > (cat arrow), in order to trigger file change detection of logstash
                    jsonFN = '{}.json'.format(srcFN) 
                    jsonFP = os.path.join(unpackPath, jsonFN)
                    cmd = 'cat {} > {}'.format(lowerkeyFP , jsonFP)
                    logger.info(cmd)
                    subprocess.call(cmd, shell=True)
                     
                
                #-- suspend until data sync to ES via logstash
                url = ImportGOCC2es.URL_ES_GOCC_COUNT.format(codename, gocc_date)
                logger.info(url)
                utility.returnOnlyIfCountStable_es(url)

                if gocc_date:
                    obj = '{}_gocc_{}'.format(codename, gocc_date)
                    msgObjs = [ obj ]
                    
                    #-- publish message    
                    hmsg = HMessage()
                    hmsg.set_codename(codename)
                    hmsg.set_eventType(EnumEvent.OBJECT_FINALIZE)
                    hmsg.set_objectIds(msgObjs)
                    logger.info(hmsg)
                    self.pub_message(ImportGOCC2es.PUB_TOPIC, [hmsg])
            
 
###                    #-- copy to local
###                    #-- >, arrow to trigger file change detection of logstash
###                    jsonPath = os.path.join(jsonPath, jsonGoodsFN)
###                    cmd = 'gsutil cat {} > {}'.format(gsJsonGoodsPath, jsonPath)
###                    logger.info(cmd)
###                    subprocess.call(cmd, shell=True)
###


