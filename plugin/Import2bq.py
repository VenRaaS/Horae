import csv
import logging
import io
import json
import os
import re
import sys
import subprocess
import datetime
import Task

file_path = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.realpath(os.path.join(file_path, os.pardir, 'lib'))
if not lib_path in sys.path : sys.path.append(lib_path)
from event import EnumEvent, EnumTopic
from subscr import EnumSubscript


class Import2bq(Task.Task):
    INVOKE_INTERVAL_SEC = 600
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent.OBJECT_FINALIZE.name ]
    PUB_TOPIC = EnumTopic['bigquery_unima_gocc']

    def exe(self, hmsg) :
        if hmsg.get_attributes() :
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if event_type in LISTEN_EVENTS:
                bucketId = attributes['bucketId'] if 'bucketId' in attributes else ''
                objectId = attributes['objectId'] if 'objectId' in attributes else ''
                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                self.logger.info('%s %s %s %s', event_type, bucketId, objectId, generation)

                #-- valid root path
                if objectId.split('/')[0] == 'tmp' or objectId.split('/')[0] == 'gocc': 
#                if objectId.split('/')[0] == 'gocc' : 
                    m = re.match(r'gocc_(\d{8})\.tar\.gz$', objectId[-20:]) 

                    #-- valid file name 
                    if m :
                        date = m.group(1)
                        codename = bucketId.split('-')[-1]

                        folder = '_'.join([bucketId, 'gocc', date])
                        unpackPath = os.path.join('/tmp', folder)
                        cmd = 'rm -rf {}'.format(unpackPath) 
                        subprocess.call(cmd.split(' '))
                        self.logger.info(cmd)
                        
                        cmd = 'mkdir -p {}'.format(unpackPath)
                        subprocess.call(cmd.split(' '))
                        self.logger.info(cmd)

                        bkName = 'gs://' + os.path.join(bucketId, objectId)
                        cmd = 'gsutil cp {} {}'.format(bkName, unpackPath)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        tarPath = os.path.join(unpackPath, objectId.split('/')[-1])
                        cmd = 'tar -xvf {} -C {}'.format(tarPath, unpackPath)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        dataPath = os.path.join(unpackPath, 'data')

                        #-- check file format
                        self.check_num_fields(dataPath)
                        self.check_file_encoding(dataPath)

                        #-- replace Double Quote \" to Space
                        dataFiles = os.path.join(dataPath, '*')
                        cmd = "sed -i 's/\"/ /g' {}".format(dataFiles)
                        self.logger.info(cmd)
                        # subprocess - https://docs.python.org/2/library/subprocess.html#using-the-subprocess-module
                        subprocess.call([cmd], shell=True)

                       
                        #-- copy to GCS 
                        gsDataPath = os.path.join('gs://', bucketId, 'tmp', 'gocc', date)
                        cmd = 'gsutil cp {} {}'.format(dataFiles, gsDataPath)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                       
                        gsPath_goods = os.path.join(gsDataPath, 'Goods.tsv')
                        cmd = 'bq load --source_format=CSV --F=''\t'' --replace --max_bad_records=10 {}_tmp.unima_goods_{} {} GID:string,PGID:string,GOODS_NAME:string,GOODS_KEYWORD:string,GOODS_BRAND:string,GOODS_DESCRIBE:string,GOODS_SPEC:string,GOODS_IMG_URL:string,AVAILABILITY:string,CURRENCY:string,SALE_PRICE:string,PROVIDER:string,BARCODE_EAN13:string,BARCODE_UPC:string,FIRST_RTS_DATE:string,UPDATE_TIME:string'.format(codename, date, gsPath_goods)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        gsPath_category = os.path.join(gsDataPath, 'Category.tsv')
                        cmd = 'bq load --source_format=CSV --F=''\t'' --replace --max_bad_records=10 {}_tmp.unima_category_{} {} CATEGORY_NAME:string,CATEGORY_CODE:string,P_CATEGORY_CODE:string,LE:string,UPDATE_TIME:string'.format(codename, date, gsPath_category)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        gsPath_gcc = os.path.join(gsDataPath, 'GoodsCateCode.tsv')
                        cmd = 'bq load --source_format=CSV --F=''\t'' --replace --max_bad_records=10 {}_tmp.unima_goods_cate_code_{} {} GID:string,CATEGORY_CODE:string,LE:string,SORT:string,FUNC_TYPE:string,INSERT_DATE:string,DISPLAY_START_DATE:string,DISPLAY_END_DATE:string,UPDATE_TIME:string'.format(codename, date, gsPath_gcc)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        dt = datetime.datetime.strptime(date, '%Y%m%d')
                        yest_date = (dt.date() - datetime.timedelta(days=1)).strftime('%Y%m%d')
                        gsPath_order = os.path.join(gsDataPath, 'OrderList_{}.tsv'.format(yest_date))
                        cmd = 'bq load --source_format=CSV --F=''\t'' --replace --max_bad_records=10 {}_tmp.OrderList_{} {} UID:string,ORDER_NO:string,SEQ:string,ORDER_DATE:DATETIME,GID:string,CURRENCY:string,SALE_PRICE:float,FINAL_PRICE:float,QTY:integer,FINAL_AMT:float,PROMO_ID:string,AFFILIATE_ID:string,DC_PRICE:float,DELIVERY_TYPE:string,UPDATE_TIME:DATETIME'.format(codename, yest_date, gsPath_order)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        #-- clean tmp folder in GCS 
                        cmd = 'gsutil rm -r -f {}'.format(gsDataPath)
                        self.logger.info(cmd)
#                        subprocess.call(cmd.split(' '))

    def check_file_encoding(self, dirPath) :
        for fname in os.listdir(dirPath):
            if fname.endswith('.csv') or fname.endswith('.tsv'):

                fpath = os.path.join(dirPath, fname) 
                try:
                    with io.open(fpath, 'r', encoding='utf-8') as f :
                        f.readlines()
                except UnicodeDecodeError:
                    self.logger.error(traceback.format_exc())

    def check_num_fields(self, dirPath) :
        for fname in os.listdir(dirPath):
            if fname.endswith('.csv') or fname.endswith('.tsv'):

                fpath = os.path.join(dirPath, fname) 
                with open(fpath, 'rb') as f :
                    reader = csv.reader(f, delimiter='\t')
                
                    num_fields_1st_row = 0
                    for fields in reader :
                        if 1 == reader.line_num : 
                            num_fields_1st_row = len(fields)
                        else :
                             if len(fields) != num_fields_1st_row :
                                 self.logger.error("line {} => {}, num of delimiters check failed!".format(reader.line_num, len(fields)))


if '__main__' == __name__:
    class MockMsg() :
        def __init__(self):
           self.attributes = {'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}
    
    from hmessage import HMessage
    hmsg = HMessage(MockMsg())  
    t = Import2bq(hmsg)
    t.start()
    t.join()
