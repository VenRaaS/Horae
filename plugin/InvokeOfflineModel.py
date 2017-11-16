import logging
import subprocess
import os
import io
import re
import csv

import Task


class InvokeOfflineModel(Task.Task):
    def exe(self, msg) :
        if hasattr(msg, 'attributes'):
            attributes = msg.attributes
            event_type = attributes['eventType']
            
            if 'OBJECT_FINALIZE' == event_type:            
                bucket_id = attributes['bucketId'] if 'bucketId' in attributes else ''
                object_id = attributes['objectId'] if 'objectId' in attributes else ''
                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                self.logger.info('%s %s %s %s', event_type, bucket_id, object_id, generation)

                #-- valid root path
                if object_id.split('/')[0] == 'tmp' or object_id.split('/')[0] == 'gocc': 
#                if object_id.split('/')[0] == 'gocc' : 
                    m = re.match(r'gocc_(\d{8})\.tar\.gz$', object_id[-20:]) 

                    #-- valid file name 
                    if m :
                        date = m.group(1)
                        code_name = bucket_id.split('-')[-1]
                        
                        cmd = '/home/itri/batch_data_ingestion/venraas/batch_data_download/gocc/run-sync.sh {} | logger '.format(code_name, date)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))

                        cmd = 'sh /home/itri/VenOfflineModule/Modules/RaaS_System_Module_BigQuery/process/daily_company_process.sh {} | logger '.format(code_name, date)
                        self.logger.info(cmd)
                        subprocess.call(cmd.split(' '))
                        


if '__main__' == __name__:
    class Msg() :
        def __init__(self):
           self.attributes = {'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}

    t = InvokeOfflineModel(Msg())
    t.start()
    t.join()
