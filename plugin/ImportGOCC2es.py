import csv
import logging
import io
import json
import os
import re
import sys
import subprocess
import datetime
import logging

from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
import lib.utility as utility
import plugin.Task as Task


logger = logging.getLogger(__file__)


class ImportGOCC2es(Task.Task):
    INVOKE_INTERVAL_SEC = 600
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bigquery'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]

    SQL_EXPORT_UNIMA_GOODS = 'SELECT \'{}\' as code_name, gid, pgid, goods_name, goods_keyword, goods_brand, goods_describe, goods_spec, goods_img_url, goods_page_url,availability, currency, sale_price, provider, barcode_ean13, barcode_upc, SUBSTR(CAST(first_rts_date AS STRING),0,19) as first_rts_date, SUBSTR(CAST(update_time AS STRING),0,19) AS update_time  FROM {}'
    
    SQL_EXPORT_UNIMA_CATEGORY = 'SELECT \'{}\' as code_name, category_code, p_category_code, category_name, le, SUBSTR(CAST(update_time AS STRING),0,19) as update_time  FROM {}'


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
                cmd = 'rm -rf {}'.format(unpackPath)
                subprocess.call(cmd.split(' '))
                logger.info(cmd)
                
                cmd = 'mkdir -p {}'.format(unpackPath)
                subprocess.call(cmd.split(' '))
                logger.info(cmd)

                bucketId = 'ven-cust-{}'.format(codename)
                gsDataPath = os.path.join('gs://', bucketId, 'tmp', 'gocc2es')

                for t in objectIds:
                    srcDS, srcTb = t.split('.')
                    if not srcTb.startswith('goods') and not srcTb.startswith('category'):
                        continue

                    #-- export to GCS
                    srcFN = srcTb
                    gsJsonPath = os.path.join(gsDataPath, srcFN)
                    cmd = 'bq extract --destination_format=NEWLINE_DELIMITED_JSON \"{}\" {}'.format(t, gsJsonPath)
                    logger.info(cmd)
                    subprocess.call(cmd, shell=True)

                    #-- download to local
                    cmd = 'gsutil cp {} {}'.format(gsJsonPath, unpackPath)
                    logger.info(cmd)
                    subprocess.call(cmd.split(' '))

                    #-- lowercase of json KEY, i.e. field name
                    jsonFN = '{}_{}.json'.format(codename, srcFN)
                    jsonFP = os.path.join(unpackPath, jsonFN)

                    with io.open(jsonFP, 'w', encoding='utf-8') as fo:
                        rawFP = os.path.join(unpackPath, srcFN)
                        with io.open(rawFP, 'r', encoding='utf-8') as fi: 
                            for line in fi:
                                o = json.loads(line)
                                o_lowerkey = dict( (k.lower(), v) for k, v in o.iteritems() )
                                fo.write(json.dumps(o_lowerkey, ensure_ascii=False) + '\n')

###                    #-- copy to local
###                    #-- >, arrow to trigger file change detection of logstash
###                    jsonPath = os.path.join(jsonPath, jsonGoodsFN)
###                    cmd = 'gsutil cat {} > {}'.format(gsJsonGoodsPath, jsonPath)
###                    logger.info(cmd)
###                    subprocess.call(cmd, shell=True)
###
                    #-- clean tmp folder in GCS 
                    cmd = 'gsutil rm -r -f {}'.format(gsDataPath)
                    logger.info(cmd)
                    subprocess.call(cmd.split(' '))

#                    cmd = 'rm -rf {}'.format(unpackPath) 
#                    logger.info(cmd)
#                    subprocess.call(cmd.split(' '))

    def check_file_encoding(self, dirPath, dataFNs):
        for fn in dataFNs:
            if fn.endswith('.csv') or fn.endswith('.tsv'):

                fpath = os.path.join(dirPath, fn) 
                try:
                    with io.open(fpath, 'r', encoding='utf-8') as f :
                        f.readlines()
                except UnicodeDecodeError:
                    logger.error(traceback.format_exc())
                    return False
        return True

    def check_num_fields(self, dirPath, dataFNs):
        for fn in dataFNs:
            if fn.endswith('.csv') or fn.endswith('.tsv'):

                fpath = os.path.join(dirPath, fn) 
                with open(fpath, 'rb') as f :
                    reader = csv.reader(f, delimiter='\t')
                
                    num_fields_1st_row = 0
                    for fields in reader :
                        if 1 == reader.line_num : 
                            num_fields_1st_row = len(fields)
                        else :
                             if len(fields) != num_fields_1st_row :
                                logger.error("line {} => {}, num of delimiters check failed!".format(reader.line_num, len(fields)))
                                return False
        return True
    
    def dataCorrection(self, dirPath, dataFNs):
        for fn in dataFNs:
            baseName = os.path.splitext(fn)[0]
            ffn = os.path.join(dirPath, fn)

            utility.remove_dq2space(ffn)
            utility.remove_zero_datetime(ffn)
            utility.lowercase_firstLine(ffn)

            if baseName.lower().endswith('goods'):
                #-- prevent cast string to Float by BQ --autodetect
                #   e.g. PGID: 5787509,5789667 => 5787509, 5789667
                utility.replace_c_cs(ffn)


if '__main__' == __name__:
    class MockMsg() :
        def __init__(self):
           self.attributes = {'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}
    
    from hmessage import HMessage
    hmsg = HMessage(MockMsg())  
    t = Update2es(hmsg)
    t.start()
    t.join()