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
from lib.event import EnumEvent, EnumTopic
from lib.subscr import EnumSubscript
from lib.tstatus import TaskStatus 
import lib.pull_pub


class Task(threading.Thread) :
    isTask = True

    #-- configuration
    INVOKE_INTERVAL_SEC = 30
    LISTEN_SUBSCRIPTS = [ EnumSubscript['pull_bucket_ven-custs'] ]
    LISTEN_EVENTS = [ EnumEvent.OBJECT_FINALIZE ]
    PUB_TOPIC = EnumTopic.bigquery


    def __init__(self, sub_msg) :
        threading.Thread.__init__(self)

        #-- task status
        self.st = TaskStatus()
        self.st.init() 

        #-- message pull from subscription
        self.sub_msg = sub_msg

        #-- logging setup
        formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(filename)s(%(lineno)s) %(name)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        if len(self.logger.handlers) <= 0:
            self.logger.addHandler(ch)

    def exe(self, msg) :
        self.logger.info(msg.get_attributes())
#        if hasattr(msg, 'attributes'):
#            self.logger.info(msg.attributes)

#        if hasattr(msg, 'data'):
#            self.logger.info(msg.data)
    
    #-- thread entry point
    #   https://docs.python.org/2/library/threading.html#thread-objects
    def run(self) :
        try:
            self.st.start()
            self.exe(self.sub_msg)

        except Exception as e:
            self.logger.error(e, exc_info=True)

        finally: 
            self.st.end()
            self.logger.info(self.st.state)
    
    ## A helper function to publish a message
    ## usage:
    ##      pubMsg = {'attributes': {'bucketId':'TT Bucket', 'objectId':'OBJ ID', 'eventType':EnumEvent.OBJECT_FINALIZE.name} }
    ##      self.pub_message(EnumTopic['bucket_ven-custs'], pubMsg)
    def pub_message(self, topic_enum, msg):
        try:
            if not topic_enum in list(EnumTopic):
                raise ValueError('the publish topic is not a EnumTopic')

            if None == msg:
                raise ValueError('input msg must not be empty')
            
            credentials = GoogleCredentials.get_application_default()
            if credentials.create_scoped_required():
                credentials = credentials.create_scoped(pull_pub.PUBSUB_SCOPES)
            client = discovery.build('pubsub', 'v1', credentials=credentials)
 
            pull_pub.publish_message(client, topic_enum, msg)
            
        except Exception as e:
            self.logger.error(e, exc_info=True)


if '__main__' == __name__:
    t = Task('local testing message')
    t.start()
    t.join()

