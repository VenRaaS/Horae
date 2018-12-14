import logging
import os
import sys
import threading
import time
from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

#file_path = os.path.dirname(os.path.realpath(__file__))
#lib_path = os.path.realpath(os.path.join(file_path, os.pardir, 'lib'))
#if not lib_path in sys.path : sys.path.append(lib_path)
from lib.event import EnumEvent
from lib.topic import EnumTopic
from lib.subscr import EnumSubscript
from lib.tstatus import TaskStatus 
import lib.pull_pub as pull_pub


#logger = logging.getLogger(__name__)


class Task(threading.Thread) :
    isTask = True

    #-- configuration
    INVOKE_INTERVAL_SEC = 30
    LISTEN_SUBSCRIPTS = []
    LISTEN_EVENTS = []
    PUB_TOPIC = None
#    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'], EnumSubscript['pull_bigquery'] ]
#    LISTEN_EVENTS = [ EnumEvent.OBJECT_FINALIZE ]
#    PUB_TOPIC = EnumTopic.bigquery


    def __init__(self, sub_msg=None) :
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(__name__)

        #-- task status
        self.st = TaskStatus()
        self.st.init() 

        #-- message pull from subscription
        self.sub_msg = sub_msg

    def exe(self, msg=None) :
        self.logger.info(msg.get_attributes())
#        if hasattr(msg, 'attributes'):
#            self.logger.info(msg.attributes)
#        if hasattr(msg, 'data'):
#            self.logger.info(msg.data)
    
    ## thread entry point
    ##   https://docs.python.org/2/library/threading.html#thread-objects
    def run(self) :
        try:
            self.st.start()
            if self.sub_msg:
                self.exe(self.sub_msg)
            else:
                self.exe()

        except Exception as e:
            self.logger.error(e, exc_info=True)

        finally: 
            self.st.end()
            self.logger.info(self.st.state)
    
    ## A helper function to publish a message
    ## usage:
    ##      pubMsg = {'attributes': {'codename':'sohappy', 'bucketId':'TT Bucket', 'objectId':'OBJ ID', 'eventType':EnumEvent.OBJECT_FINALIZE.name} }
    ##      self.pub_message(EnumTopic['bucket_ven-custs'], pubMsg)
    def pub_message(self, topic_enum, hmsgs):
        try:
            if not topic_enum in list(EnumTopic):
                raise ValueError('the publish topic is not a EnumTopic')

            if None == hmsgs:
                raise ValueError('input msg must not be empty')
            
            credentials = GoogleCredentials.get_application_default()
            if credentials.create_scoped_required():
                credentials = credentials.create_scoped(pull_pub.PUBSUB_SCOPES)
            client = discovery.build('pubsub', 'v1', credentials=credentials)
 
            pull_pub.publish_message(client, topic_enum, hmsgs)
            
        except Exception as e:
            self.logger.error(e, exc_info=True)


if '__main__' == __name__:
    t = Task('local testing message')
    t.start()
    t.join()

