import csv
import logging
import io
import json
import os
import re
import sys
import subprocess
import datetime
import plugin.Task as Task

from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
import lib.utility as utility


logger = logging.getLogger(__name__)


class UpdateGoods2es(Task.Task):
    INVOKE_INTERVAL_SEC = 60 * 3
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]

    HEADER_GOODS = 'GID	PGID	GOODS_NAME	GOODS_KEYWORD	GOODS_BRAND	GOODS_DESCRIBE	GOODS_SPEC	GOODS_IMG_URL	AVAILABILITY	CURRENCY	SALE_PRICE	PROVIDER	BARCODE_EAN13	BARCODE_UPC	FIRST_RTS_DATE	UPDATE_TIME'

    SQL2UNIMA_GOODS = 'SELECT SAFE_CAST(gid AS string) AS gid, SAFE_CAST(pgid AS string) AS pgid, SAFE_CAST(availability AS string) AS availability, SAFE_CAST(sale_price AS string) AS sale_price, SAFE_CAST(provider AS string) AS provider, SAFE_CAST(first_rts_date AS datetime) AS first_rts_date, SAFE_CAST(update_time AS datetime) AS update_time,  * except (gid, pgid, availability, sale_price, provider, first_rts_date, update_time) FROM {ds}.{tb}'
    
    SQL_FORM_EXPORT_GOODS = 'SELECT \'{cn}\' as code_name, SUBSTR(CAST(first_rts_date AS STRING),0,19) as first_rts_date, SUBSTR(CAST(update_time AS STRING),0,19) AS update_time,  * EXCEPT (first_rts_date, update_time) from {ds}.{tb}'


    def exe(self, hmsg) :
        if hmsg.get_attributes() :
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if 'OBJECT_FINALIZE' == event_type:            
                bucketId = attributes['bucketId'] if 'bucketId' in attributes else ''
                objectId = attributes['objectId'] if 'objectId' in attributes else ''
                codename = bucketId.split('-')[-1]

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', event_type, bucketId, objectId, generation)

                #-- valid path in GCS
                #   gocc/update/sohappy_gocc_20180111.tar.gz
                gsPaths = objectId.split('/')
                if 3 == len(gsPaths) and gsPaths[0] == 'gocc' and gsPaths[1] == 'update' :
                    m = re.match(r'gocc_(\d{8})\.tar\.gz$', objectId[-20:]) 

                    #-- valid file name 
                    if m :
                        date = m.group(1)
                        time = datetime.datetime.now().strftime('%H%M')

                        folder = '_'.join([bucketId, 'gocc', 'update', date])
                        unpackPath = os.path.join('/tmp', folder)
                        cmd = 'rm -rf {}'.format(unpackPath) 
                        subprocess.call(cmd.split(' '))
                        logger.info(cmd)
                        
                        cmd = 'mkdir -p {}'.format(unpackPath)
                        subprocess.call(cmd.split(' '))
                        logger.info(cmd)

                        bkName = 'gs://' + os.path.join(bucketId, objectId)
                        cmd = 'gsutil cp {} {}'.format(bkName, unpackPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        tarPath = os.path.join(unpackPath, objectId.split('/')[-1])
                        cmd = 'tar -xvf {} -C {}'.format(tarPath, unpackPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        
                        dataPath = os.path.join(unpackPath, 'update_data')

                        #-- data filenames 
                        dataFNs = [ os.path.basename(fn) for fn in os.listdir(dataPath) if utility.basename(fn).lower().endswith('goods') ]
                        logger.info(dataFNs)

                        self.remove_double_quote(dataPath, dataFNs)
                        #-- check file format
                        if not self.check_num_fields(dataPath, dataFNs): return
                        if not self.check_file_encoding(dataPath, dataFNs): return

                        #-- data correction
                        self.dataCorrection(dataPath, dataFNs)

                        #-- insert Header 
                        for fn in dataFNs :
                            ffn = os.path.join(dataPath, fn)
                            logger.info('%s has_header: %s', ffn, utility.has_header(ffn))

                            if not utility.has_header(ffn):
                                cmd = "sed -i '1s/^/{}\\n/' {}"
                                if ffn.endswith('Goods.tsv'): 
                                    cmd = cmd.format(UpdateGoods2es.HEADER_GOODS, ffn)

                                logger.info(cmd)
                                subprocess.call([cmd], shell=True)
                       
                        #-- copy to GCS
                        gsTmpFolder = '_'.join(['gocc', date])
                        gsDataPath = os.path.join('gs://', bucketId, 'tmp', gsTmpFolder)
                        dataFiles = os.path.join(dataPath, '*')
                        cmd = 'gsutil cp {} {}/'.format(dataFiles, gsDataPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        #-- load into BQ tmp dataset
                        for fn in dataFNs:
                            gsPath = os.path.join(gsDataPath, fn)
                            baseName = os.path.splitext(fn)[0]

                            dataset = '{}_tmp'.format(codename)
                            extTb = 'update_ext_{}'.format(baseName.lower())

                            if extTb.endswith('goods'):
                                cmd = 'bq load --autodetect --replace --source_format=CSV --field_delimiter=''\t'' {}.{} {}'.format(dataset, extTb, gsPath)
                                logger.info(cmd)
                                out = subprocess.check_output(cmd.split(' '))
                                logger.info(out)
                                break
 
                        #-- form with unima schema
                        for fn in dataFNs:
                            tmpDS = '{}_tmp'.format(codename)
                            baseName = os.path.splitext(fn)[0]
                            extTb = 'update_ext_{}'.format(baseName.lower())
                            logger.info(extTb)

                            dataset = '{}_tmp'.format(codename)
                            unimaTb = 'update_{name}'.format(name=baseName.lower())

                            if extTb.endswith('goods'):
                                sql = UpdateGoods2es.SQL2UNIMA_GOODS.format(ds=tmpDS, tb=extTb)
                                cmd = 'bq query -n 0 --replace --use_legacy_sql=False --destination_table={}.{} {}'.format(dataset, unimaTb, sql)
                                logger.info(cmd)
                                out = subprocess.check_output(cmd.split(' '))
                                logger.info(out)

                                exportTmpTb = '{cn}_tmp.update_export_goods_{d}{t}'.format(cn=codename, d=date, t=time)
                                sql = UpdateGoods2es.SQL_FORM_EXPORT_GOODS.format(cn=codename, ds=tmpDS, tb=unimaTb)
                                cmd = 'bq query -n 0 --nouse_legacy_sql --replace --destination_table=\"{}\" \"{}\"'.format(exportTmpTb, sql)
                                logger.info(cmd)
                                out = subprocess.check_output(cmd, shell=True)
                                logger.info(out)
                                cmd = 'bq update --expiration 259200 {tb}'.format(tb=exportTmpTb)
                                logger.info(cmd)
                                out = subprocess.check_output(cmd, shell=True)
                                logger.info(out)

                                jsonGoodsFN = "{}_goods_{}.json".format(codename, date)
                                gsJsonGoodsPath = os.path.join('gs://', bucketId, 'tmp', gsTmpFolder, jsonGoodsFN)
                                cmd = 'bq extract --destination_format=NEWLINE_DELIMITED_JSON \"{}\" {}'.format(exportTmpTb, gsJsonGoodsPath)
                                logger.info(cmd)
                                out = subprocess.check_output(cmd, shell=True)
                                logger.info(out)
                                break


                        #-- local json path
                        jsonPath = '/tmp/gocc2es_update'
                        cmd = 'mkdir -p {}'.format(jsonPath)
                        logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        #-- copy to local
                        #-- >, arrow to trigger file change detection of logstash
                        jsonPath = os.path.join(jsonPath, jsonGoodsFN)
                        cmd = 'gsutil cat {} > {}'.format(gsJsonGoodsPath, jsonPath)
                        logger.info(cmd)
                        subprocess.call(cmd, shell=True)

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
            #subprocess.call(["sed", "-i", 's/"//g', fpath])
            utility.remove_dq2space(fpath)
        out = subprocess.check_output(["ls", "-l", dirPath])
        logger.info(out)
        return True

    def check_file_encoding(self, dirPath, dataFNs):
        for fn in dataFNs:
            if fn.endswith('.csv') or fn.endswith('.tsv'):
                fpath = os.path.join(dirPath, fn) 
                logger.info('begin check_file_encoding '+fpath)
                out = subprocess.check_output(["file", "-bi", fpath])
                logger.info(out)
                #return (out.find('utf-8') >0)
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
                                logger.error(fields)
                                return False
        return True
    
    def dataCorrection(self, dirPath, dataFNs):
        for fn in dataFNs:
            baseName = os.path.splitext(fn)[0]
            ffn = os.path.join(dirPath, fn)
            #logger.info("begin remove_dq2space "+ffn)
            #utility.remove_dq2space(ffn)
            logger.info("begin remove_zero_datetime "+ffn)
            utility.remove_zero_datetime(ffn)
            logger.info("begin lowercase_firstLine "+ffn)
            utility.lowercase_firstLine(ffn)

            if baseName.lower().endswith('goods'):
                #-- prevent cast string to Float by BQ --autodetect
                #   e.g. PGID: 5787509,5789667 => 5787509, 5789667
                logger.info("begin replace_c_cs "+ffn)
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
