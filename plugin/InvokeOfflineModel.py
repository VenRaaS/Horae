import logging
import subprocess
import os
import io
import re
import sys
import Task

file_path = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.realpath(os.path.join(file_path, os.pardir, 'lib'))
if not lib_path in sys.path : sys.path.append(lib_path)
from event import EnumEvent, EnumTopic
from subscr import EnumSubscript


class InvokeOfflineModel(Task.Task):
    INVOKE_INTERVAL_SEC = 36000
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]

    def exe(self, hmsg) :
        if hasattr(hmsg.msg, 'attributes'):
            attributes = hmsg.msg.attributes
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
#                        subprocess.call(cmd.split(' '))

                        cmd = 'sh /home/itri/VenOfflineModule/Modules/RaaS_System_Module_BigQuery/process/daily_company_process.sh {} | logger '.format(code_name, date)
                        self.logger.info(cmd)
#                        subprocess.call(cmd.split(' '))
                        


if '__main__' == __name__:
    class MockMsg() :
        def __init__(self):
           self.attributes = {'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}
    
    from hmessage import HMessage
    hmsg = HMessage(MockMsg())  
    t = InvokeOfflineModel(hmsg)
    t.start()
    t.join()
