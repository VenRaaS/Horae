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


class UpdateGoods2es(Task.Task):
    INVOKE_INTERVAL_SEC = 600
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]
    PUB_TOPIC = EnumTopic.es_cluster

    def exe(self, hmsg) :
        if hmsg.get_attributes() :
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if 'OBJECT_FINALIZE' == event_type:            
                bucketId = attributes['bucketId'] if 'bucketId' in attributes else ''
                objectId = attributes['objectId'] if 'objectId' in attributes else ''
                codename = bucketId.split('-')[-1]

                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                self.logger.info('%s %s %s %s', event_type, bucketId, objectId, generation)

                #-- valid path in GCS
                gsPaths = objectId.split('/')
                if 3 == len(gsPaths) and gsPaths[0] == 'gocc' and gsPaths[1] == 'update' :
                    m = re.match(r'gocc_(\d{8})\.tar\.gz$', objectId[-20:]) 

                    #-- valid file name 
                    if m :
                        date = m.group(1)

                        folder = '_'.join([bucketId, 'gocc', 'update', date])
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
                        return
                        

                        #-- check file format
                        self.check_num_fields(dataPath)
                        self.check_file_encoding(dataPath)

                        #-- replace Double Quote \" to Space
                        dataFiles = os.path.join(dataPath, '*')
                        cmd = "sed -i 's/\"/ /g' {}".format(dataFiles)
                        self.logger.info(cmd)
                        # subprocess - https://docs.python.org/2/library/subprocess.html#using-the-subprocess-module
                        subprocess.call([cmd], shell=True)


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
    t = Update2es(hmsg)
    t.start()
    t.join()
