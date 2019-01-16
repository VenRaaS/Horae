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
from lib.hmessage import HMessage
import lib.utility as utility
import plugin.Task as Task


logger = logging.getLogger(__name__)


class ImportGOCC2bq(Task.Task):
    INVOKE_INTERVAL_SEC = 60 * 20
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent.OBJECT_FINALIZE ]
    PUB_TOPIC = EnumTopic.bigquery

    HEADER_GOODS = 'GID	PGID	GOODS_NAME	GOODS_KEYWORD	GOODS_BRAND	GOODS_DESCRIBE	GOODS_SPEC	GOODS_IMG_URL	AVAILABILITY	CURRENCY	SALE_PRICE	PROVIDER	BARCODE_EAN13	BARCODE_UPC	FIRST_RTS_DATE	UPDATE_TIME'
    HEADER_ORDERLIST = 'UID	ORDER_NO	SEQ	ORDER_DATE	GID	CURRENCY	SALE_PRICE	FINAL_PRICE	QTY	FINAL_AMT	PROMO_ID	AFFILIATE_ID	DC_PRICE	DELIVERY_TYPE	UPDATE_TIME'
    HEADER_CATEGORY = 'CATEGORY_NAME	CATEGORY_CODE	P_CATEGORY_CODE	LE	UPDATE_TIME'
    HEADER_CUSTOMER = 'UID	GENDER	B_DATE	COUNTRY	AREA_1	AREA_2	EDM_YN	EMAIL	UPDATE_TIME'
    HEADER_GOODSCATECODE = 'GID	CATEGORY_CODE	LE	SORT	FUNC_TYPE	INSERT_DATE	DISPLAY_START_DATE	DISPLAY_END_DATE	UPDATE_TIME'

    SQL2UNIMA_GOODS = 'SELECT SAFE_CAST(GID AS string) AS GID, SAFE_CAST(PGID as string) AS PGID, SAFE_CAST(AVAILABILITY AS string) AS AVAILABILITY, SAFE_CAST(SALE_PRICE AS string) AS SALE_PRICE, SAFE_CAST(PROVIDER as string) AS PROVIDER, SAFE_CAST(FIRST_RTS_DATE AS DATETIME) AS FIRST_RTS_DATE, SAFE_CAST(UPDATE_TIME as DATETIME) AS UPDATE_TIME,  * EXCEPT (GID, PGID, AVAILABILITY, SALE_PRICE, PROVIDER, FIRST_RTS_DATE, UPDATE_TIME) FROM {}.{}'
    SQL2UNIMA_CATEGORY = 'SELECT SAFE_CAST(CATEGORY_CODE AS string) AS CATEGORY_CODE, SAFE_CAST(P_CATEGORY_CODE AS string) AS P_CATEGORY_CODE, SAFE_CAST(LE AS string) AS LE, SAFE_CAST(UPDATE_TIME AS DATETIME) AS UPDATE_TIME,  * EXCEPT (CATEGORY_CODE, P_CATEGORY_CODE, LE, UPDATE_TIME) FROM {}.{}'
    SQL2UNIMA_GOODSCATECODE = 'SELECT SAFE_CAST(GID AS string) AS GID, SAFE_CAST(CATEGORY_CODE AS string) AS CATEGORY_CODE, SAFE_CAST(LE AS string) AS LE, SAFE_CAST(INSERT_DATE AS DATETIME) AS INSERT_DATE, SAFE_CAST(DISPLAY_START_DATE AS DATETIME) AS DISPLAY_START_DATE, SAFE_CAST(DISPLAY_END_DATE AS DATETIME) AS DISPLAY_END_DATE, SAFE_CAST(UPDATE_TIME AS DATETIME) AS UPDATE_TIME,  * EXCEPT (GID, CATEGORY_CODE, LE, INSERT_DATE, DISPLAY_START_DATE, DISPLAY_END_DATE, UPDATE_TIME) FROM {}.{}'
    SQL2UNIMA_CUSTOMER = 'SELECT SAFE_CAST(UID AS string) AS UID, case when EDM_YN is true then "Y" else "N" END AS EDM_YN, SAFE_CAST(UPDATE_TIME AS DATETIME) AS UPDATE_TIME,  * EXCEPT (UID, EDM_YN, UPDATE_TIME) FROM {}.{}'
    SQL2UNIMA_ORDERLIST = 'SELECT SAFE_CAST(UID AS string) AS UID, SAFE_CAST(SEQ AS string) AS SEQ, SAFE_CAST(ORDER_DATE AS DATETIME) AS ORDER_DATE, SAFE_CAST(GID AS string) AS GID, SAFE_CAST(SALE_PRICE AS FLOAT64) AS SALE_PRICE, SAFE_CAST(FINAL_PRICE AS FLOAT64) AS FINAL_PRICE, SAFE_CAST(FINAL_AMT AS FLOAT64) AS FINAL_AMT, SAFE_CAST(DC_PRICE AS FLOAT64) AS DC_PRICE, SAFE_CAST(UPDATE_TIME AS DATETIME) AS UPDATE_TIME,  * EXCEPT (UID, SEQ, ORDER_DATE, GID, SALE_PRICE, FINAL_PRICE, FINAL_AMT, DC_PRICE, UPDATE_TIME) FROM {}.{}'


    def exe(self, hmsg) :
        if hmsg.get_attributes() :
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if EnumEvent[event_type] in ImportGOCC2bq.LISTEN_EVENTS:
                bucketId = attributes['bucketId'] if 'bucketId' in attributes else ''
                objectId = attributes['objectId'] if 'objectId' in attributes else ''
                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', event_type, bucketId, objectId, generation)

                #-- valid file path in GCS
                gsPaths = objectId.split('/')
                if 2 == len(gsPaths) and (gsPaths[0] == 'gocc' or gsPaths[0] == 'tmp'):
                    m = re.match(r'gocc_(\d{8})\.tar\.gz$', objectId[-20:]) 

                    #-- valid file name 
                    if m :
                        date = m.group(1)
                        codename = bucketId.split('-')[-1]

                        folder = '_'.join([bucketId, 'gocc', date])
                        unpackPath = os.path.join('/tmp', folder)
                        cmd = 'rm -rf {}'.format(unpackPath) 
                        subprocess.call(cmd.split(' '))
                        logger.info(cmd)
                        
                        cmd = 'mkdir -p {}'.format(unpackPath)
                        subprocess.call(cmd.split(' '))
                        logger.info(cmd)

                        bkName = 'gs://' + os.path.join(bucketId, objectId)
                        cmd = 'gsutil -m cp {} {}'.format(bkName, unpackPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        tarPath = os.path.join(unpackPath, objectId.split('/')[-1])
                        cmd = 'tar -xvf {} -C {}'.format(tarPath, unpackPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        dataPath = os.path.join(unpackPath, 'data')

                        #-- data filenames 
                        dataFNs = []
                        for fn in os.listdir(dataPath):
                            fn = os.path.basename(fn)
                            isvalid, rowcnt = self.check_file_size(dataPath, fn)
                            if not isvalid:
                                tarfn = os.path.basename(tarPath)
                                msg = 'skip file: {0} (in {1}) due to invalid row count: {2:,}'.format(fn, tarfn, rowcnt)
                                logger.warn(msg)
                                utility.warning2slack(codename, msg)
                                continue

                            dataFNs.append(os.path.basename(fn)) 

                        logger.info(dataFNs)

                        self.remove_double_quote(dataPath, dataFNs)
                        #-- check file format
                        if not self.check_num_fields(dataPath, dataFNs): return
                        #if not self.check_file_encoding(dataPath, dataFNs): return
                        #-- data correction
                        self.dataCorrection(dataPath, dataFNs)

                        #-- insert Header 
                        for fn in dataFNs :
                            ffn = os.path.join(dataPath, fn)
                            logger.info('%s has_header: %s', ffn, utility.has_header(ffn))

                            if not utility.has_header(ffn):
                                cmd = "sed -i '1s/^/{}\\n/' {}"
                                if ffn.endswith('Goods.tsv'): 
                                    cmd = cmd.format(ImportGOCC2bq.HEADER_GOODS, ffn)
                                elif 'OrderList' in os.path.split(ffn)[-1]:
                                    cmd = cmd.format(ImportGOCC2bq.HEADER_ORDERLIST, ffn)
                                elif ffn.endswith('Category.tsv'):
                                    cmd = cmd.format(ImportGOCC2bq.HEADER_CATEGORY, ffn)
                                elif ffn.endswith('Customer.tsv'):
                                    cmd = cmd.format(ImportGOCC2bq.HEADER_CUSTOMER, ffn)
                                elif ffn.endswith('GoodsCateCode.tsv'):
                                    cmd = cmd.format(ImportGOCC2bq.HEADER_GOODSCATECODE, ffn)

                                logger.info(cmd)
                                subprocess.call([cmd], shell=True)
                       
                        #-- copy to GCS
                        gsTmpFolder = '_'.join(['gocc', date])
                        gsDataPath = os.path.join('gs://', bucketId, 'tmp', gsTmpFolder + '/')
                        dataFiles = os.path.join(dataPath, '*')
                        cmd = 'gsutil -m cp {} {}'.format(dataFiles, gsDataPath)
                        logger.info(cmd)
                        out = subprocess.check_output(cmd.split(' '))
                        logger.info(out)

                        #-- load into BQ tmp dataset
                        for fn in dataFNs:
                            gsPath = os.path.join(gsDataPath, fn)

                            baseName = os.path.splitext(fn)[0]
                            tmpTb = 'ext_{}'.format(baseName.lower())
                            dataset = '{}_tmp'.format(codename)

                            cmd = 'bq load --autodetect --replace --source_format=CSV --field_delimiter=''\t'' {}.{} {}'.format(dataset, tmpTb, gsPath)
                            logger.info(cmd)
                            out = subprocess.check_output(cmd.split(' '))
                            logger.info(out)

                        msgObjs = []
                        #-- load into BQ unima dataset
                        for fn in dataFNs:
                            tmpDS = '{}_tmp'.format(codename)
                            baseName = os.path.splitext(fn)[0]
                            tmpTb = 'ext_{}'.format(baseName.lower())
                            logger.info(tmpTb)

                            unimaDS = '{}_unima'.format(codename)
                            unimaTb = '{}_{}'.format(baseName.lower(), date)

                            if tmpTb.endswith('goods'):
                                sql = ImportGOCC2bq.SQL2UNIMA_GOODS.format(tmpDS, tmpTb)
                            elif 'orderlist' in tmpTb:
                                unimaTb = '{}'.format(baseName.lower())
                                sql = ImportGOCC2bq.SQL2UNIMA_ORDERLIST.format(tmpDS, tmpTb)
                            elif tmpTb.endswith('category'):
                                sql = ImportGOCC2bq.SQL2UNIMA_CATEGORY.format(tmpDS, tmpTb)
                            elif tmpTb.endswith('goodscatecode'):
                                sql = ImportGOCC2bq.SQL2UNIMA_GOODSCATECODE.format(tmpDS, tmpTb)
                            elif tmpTb.endswith('customer'):
                                unimaTb = '{}'.format(baseName.lower())
                                sql = ImportGOCC2bq.SQL2UNIMA_CUSTOMER.format(tmpDS, tmpTb)

                            destTb = '{}.{}'.format(unimaDS, unimaTb)
                            cmd = 'bq query -n 0 --replace --use_legacy_sql=False --destination_table={} {}'.format(destTb, sql)
                            logger.info(cmd)
                            subprocess.call(cmd.split(' '))
                            
                            msgObjs.append(destTb)
                       
                        #-- publish message of uploaded data
                        hmsg = HMessage()
                        hmsg.set_codename(codename)
                        hmsg.set_eventType(EnumEvent.OBJECT_FINALIZE)
                        hmsg.set_objectIds(msgObjs)
                        logger.info(hmsg)
                        self.pub_message(self.PUB_TOPIC, [hmsg])

                        #-- clean tmp folder in GCS 
                        cmd = 'gsutil rm -r -f {}'.format(gsDataPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        cmd = 'rm -rf {}'.format(unpackPath) 
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        logger.info('happy ending')

    def remove_double_quote(self, dirPath, dataFNs):
        for fn in dataFNs:
            fpath = os.path.join(dirPath, fn) 
            logger.info('remove double quote '+fpath)
            #subprocess.call(["sed", "-i", 's/\"//g', fpath])
            utility.remove_dq2space(fpath)
        out = subprocess.check_output(["ls", "-l", dirPath])
        logger.info(out)
        return True

    def check_file_size(self, dirPath, fn):
        cnt = 0
        fpath = os.path.join(dirPath, fn) 
        logger.info('check_file_size '+fpath)
        out = subprocess.check_output(["wc", "-l", fpath])
        logger.info(out)
        cnt = int(out.split()[0])            
        return (True, cnt) if cnt < 20 * 1000*1000 else (False, cnt)

    def check_file_encoding(self, dirPath, dataFNs):
        for fn in dataFNs:
            if fn.endswith('.csv') or fn.endswith('.tsv'):
                fpath = os.path.join(dirPath, fn)
                logger.info("begin check_file_encoding "+fpath)
                out = subprocess.check_output(["file", "-bi", fpath])
                logger.info(out)
                #return (out.find('utf-8') >0) 
        return True

    def check_num_fields(self, dirPath, dataFNs):
        for fn in dataFNs:
            if fn.endswith('.csv') or fn.endswith('.tsv'):

                fpath = os.path.join(dirPath, fn)
                logger.info("begin check_num_fields "+fpath) 
                with open(fpath, 'rb') as f :
                    reader = csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE)
                
                    num_fields_1st_row = 0
                    for fields in reader :
                        if 1 == reader.line_num : 
                            num_fields_1st_row = len(fields)
                        else :
                             if len(fields) != num_fields_1st_row :
                                logger.error("{} line {} => {}, num of delimiters check failed!".format(fpath, reader.line_num, len(fields)))
                                return False
        return True
    
    def dataCorrection(self, dirPath, dataFNs):
        for fn in dataFNs:
            baseName = os.path.splitext(fn)[0]
            ffn = os.path.join(dirPath, fn)
            #logger.info("begin remove_dq2space "+ffn)
            #utility.remove_dq2space(ffn)
            logger.info("begin remove_zero_date "+ffn)
            utility.remove_zero_datetime(ffn)

            if baseName.lower().endswith('goods'):
                #-- prevent cast string to Float by BQ --autodetect
                #   e.g. PGID: 5787509,5789667 => 5787509, 5789667
                logger.info("begin replace_c_cs "+ffn)
                utility.replace_c_cs(ffn)
        
        out = subprocess.check_output(["ls", "-l", dirPath])
        logger.info(out)
        return True


if '__main__' == __name__:
    class MockMsg() :
        def __init__(self):
           self.message = {'ImportGOCC2bq':{'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}}
    
    hmsg = HMessage(MockMsg().message)  
    t = ImportGOCC2bq(hmsg)
    t.start()
    t.join()
