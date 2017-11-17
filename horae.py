#from google.cloud import pubsub
from google.cloud.pubsub_v1.subscriber.policy import thread
import grpc
from google.cloud import pubsub_v1
import time
import threading
import logging
import os
import imp
import sys

#-- lib/ and plugin/ modules paths
file_path = os.path.dirname(os.path.realpath(__file__))
plugin_path = os.path.join(file_path, 'plugin')
lib_path = os.path.join(file_path, 'lib')
if not plugin_path in sys.path: sys.path.append(plugin_path)
if not lib_path in sys.path: sys.path.append(lib_path)
import Task
from hmessage import HMessage
from subscr import EnumSubscript
from event import EnumEvent
from tstatus import EnumState

#-- logging setup
formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(filename)s(%(lineno)s) %(name)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

#fh = logging.FileHandler("{0}.log".format(__name__))
#fh.setLevel(logging.DEBUG)
#fh.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)
#logger.addHandler(fh)
 

class UnavailableIgnorePolicy(thread.Policy) :
    def on_exception(self, exception):
        """
        Ignore UNAVAILABLE.
        """
        #-- If this is UNAVAILABLE, then we want to retry.
        #   That entails just returning None.
        unavailable = grpc.StatusCode.UNAVAILABLE
        if getattr(exception, 'code', lambda: None)() == unavailable:
            logger.warn(exception)
            return

        #-- For anything else, propagate to super.
        super(UnavailableIgnorePolicy, self).on_exception(exception)


g_pluginMods = {}
def load_plugin_modules() :
    file_path = os.path.dirname(os.path.realpath(__file__))
    plugin_path = os.path.join(file_path, 'plugin')
    
    #-- remove all to reflash existing plugins 
    g_pluginMods.clear()

    modules = os.listdir(plugin_path)
    for mod_fname in modules:
        mod_name, ext = os.path.splitext(mod_fname)
        mod_path = os.path.join(plugin_path, mod_fname)

        py_mod = None
        if ext.lower() == '.py': 
            try: 
                if mod_name in g_pluginMods:
                    py_mod = imp.reload( g_pluginMods[mod_name] )
                else:
                    py_mod = imp.load_source(mod_name, mod_path) 

                g_pluginMods[mod_name] = py_mod

                logger.info("Loaded module: %s", mod_name);
            except Exception as e:
                logger.error(e, exc_info=True)


def create_subscriber(project, subscript):
    """
    Receives messages from a pull subscription.
    """
    #-- appply UnavailableIgnorePolicy to skip UNAVAILABLE
    subscriber = pubsub_v1.SubscriberClient(policy_class = UnavailableIgnorePolicy)
    subscription_path = subscriber.subscription_path(project, subscript.name)

    scb = sub_callback(subscript)
    subscriber.subscribe(subscription_path, callback=scb.callback)

    return {'client':subscriber, 'path':subscription_path}


#TODO g_lock
g_taskInstDict_lock = threading.RLock() 
g_taskInstDict = {}
class sub_callback() : 
    def __init__(self, subscript) :
        self.subscript = subscript
 
    def callback(self, msg) :
        for mod_name, py_mod in g_pluginMods.iteritems():
            try:
                #-- get class
                taskClass = getattr(py_mod, mod_name)

                hMsg = HMessage(msg)
                eventType = hMsg.get_eventType()

                #-- must inherit Task.Task
                logger.info('%s, %s issubclass of Task.Task: %s', eventType, taskClass, issubclass(taskClass, Task.Task))
                if not issubclass(taskClass, Task.Task): continue
                
                #-- math Subscription and eventType between message and plugin class (taskClass)
                if not self.subscript       in taskClass.LISTEN_SUBSCRIPTS: continue
                if not EnumEvent[eventType] in taskClass.LISTEN_EVENTS: continue

                #-- instantiate the object of plugin task class 
                k = 'task instance key: {}/{}/{}/{}'.format(self.subscript.name, eventType, hMsg.get_codename(), mod_name)
                logger.info(k)

                with g_taskInstDict_lock:
                    if not k in g_taskInstDict:
                        taskInst = taskClass(hMsg)
                        g_taskInstDict[k] = taskInst
                        taskInst.start()
                    else:
                        logger.info('skip message due to task:%s is not expired yet, %s', k, hMsg)

            except Exception as e:
                logger.error(e, exc_info=True)

        msg.ack()


def receive_messages(project, subscript):
    sub = create_subscriber(project, subscript)
    logger.info('Listening for messages on %s', sub['path'])



if '__main__' == __name__ :
    receive_messages('venraasitri', EnumSubscript['pull_bucket_ven-custs'])

    #-- The subscriber is non-blocking, so we must keep the main thread from
    #   exiting to allow it to process messages in the background.
    try:
        while True:
            load_plugin_modules()
            with g_taskInstDict_lock:
                keys = g_taskInstDict.keys()
                for k in keys:
                    taskInst = g_taskInstDict[k]
                    if EnumState.END != taskInst.st.state: continue
                    if taskInst.INVOKE_INTERVAL_SEC < taskInst.st.elapsed_afterend_sec():
                        del g_taskInstDict[k]
                        print "del ", k

            time.sleep(3)
                
    except KeyboardInterrupt:
        print "shutdown requested, exiting... "

    print 'end.'


