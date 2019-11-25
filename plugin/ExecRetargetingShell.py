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


class ExeceDMShell(Task.Task):
    INVOKE_INTERVAL_SEC = 60 * 5
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent.OBJECT_FINALIZE ]
    PUB_TOPIC = None



    def exe(self, hmsg) :
        if hmsg.get_attributes() :
            attributes = hmsg.get_attributes()
            event_type = hmsg.get_eventType()
            
            if EnumEvent[event_type] in ExeceDMShell.LISTEN_EVENTS:
                bucketId = attributes['bucketId'] if 'bucketId' in attributes else ''
                objectId = attributes['objectId'] if 'objectId' in attributes else ''
                generation = attributes['objectGeneration'] if 'objectGeneration' in attributes else ''
                logger.info('%s %s %s %s', event_type, bucketId, objectId, generation)
                
                gsPaths = objectId.split('/')
                logger.info('%s',gsPaths)
                if 4 == len(gsPaths) and gsPaths[0] == 'retargeting' and gsPaths[1] == 'edm' :
                    #subprocess.call(["sh", "/home/itri/angel/process.sh"])
                    logger.info('%s',objectId[-19:])
                    m = re.match(r'edm_(\d{8})\.tar\.gz$', objectId[-19:])
                    #logger.info('%s',m)
                    if m:
                       subprocess.call(["sh", "/home/itri/angel/process.sh", gsPaths[3]])
                elif 4 == len(gsPaths) and gsPaths[0] =='retargeting' and gsPaths[1] == 'user_clustering' :
                    #subprocess.call(["sh", "/home/itri/angel/process.sh"])
                    logger.info('%s',objectId[-18:])
                    m = re.match(r'uc_(\d{8})\.tar\.gz$', objectId[-18:])
                    #logger.info('%s',m)
                    if m:
                       subprocess.call(["sh", "/home/itri/angel/process.sh", gsPaths[3]])
                elif 4 == len(gsPaths) and gsPaths[0] =='retargeting' and gsPaths[1] == 'upg' :
                    #subprocess.call(["sh", "/home/itri/angel/process.sh"])
                    logger.info('%s',objectId[-19:])
                    m = re.match(r'uc_(\d{8})\.tar\.gz$', objectId[-19:])
                    #logger.info('%s',m)
                    if m:
                       subprocess.call(["sh", "/home/itri/angel/process.sh", gsPaths[3]])
                

if '__main__' == __name__:
    class MockMsg() :
        def __init__(self):
           self.message = {'ExeceDMShell':{'eventType':'OBJECT_FINALIZE', 'objectId':'fake message'}}
    
    hmsg = HMessage(MockMsg().message)  
    t = ExeceDMShell(hmsg)
   # t.start()
   # t.join()
