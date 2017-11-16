import logging
import os
import sys
import threading
import time

file_path = os.path.dirname(os.path.realpath(__file__))
lib_path = os.path.realpath(os.path.join(file_path, os.pardir, 'lib'))
if not lib_path in sys.path : sys.path.append(lib_path)
from event import EnumEvent, EnumTopic
from tstatus import TaskStatus 


class Task(threading.Thread) :
    #-- configuration
    INVOKE_INTERVAL_SEC = 10 
    LISTEN_EVENTS = [ EnumEvent['OBJECT_FINALIZE'] ]
    LISTEN_TOPICS = [ EnumTopic['bucket_ven-custs'] ]

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
        self.logger.info(msg)
        if hasattr(msg, 'attributes'):
            self.logger.info(msg.attributes)

        if hasattr(msg, 'data'):
            self.logger.info(msg.data)

 
    def pub(self) :
        pass
    
    #-- thread entry point
    #   https://docs.python.org/2/library/threading.html#thread-objects
    def run(self) :
        self.st.start()
        self.exe(self.sub_msg)

        self.st.pub()
        self.pub()

        self.st.end()
#        print self.st.elapsed_sec()


if '__main__' == __name__:
    t = Task('local testing message')
    t.start()
    t.join()

