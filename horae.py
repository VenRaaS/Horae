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
def load_plugin_modules():
    file_path = os.path.dirname(os.path.realpath(__file__))
    plugin_path = os.path.join(file_path, 'plugin')
    
    #-- remove all to reflash existing plugins 
    g_pluginMods.clear()

    modules = os.listdir(plugin_path)
    for mod_fname in modules :
        mod_name, ext = os.path.splitext(mod_fname)
        mod_path = os.path.join(plugin_path, mod_fname)

        py_mod = None
        if ext.lower() == '.py' : 
            try: 
                if mod_name in g_pluginMods :
                    py_mod = imp.reload( g_pluginMods[mod_name] )
                else :
                    py_mod = imp.load_source(mod_name, mod_path) 

                g_pluginMods[mod_name] = py_mod

                logger.info("Loaded module: %s", mod_fname);
            except Exception as e:
                logger.error(e, exc_info=True)


def create_subscriber(project, sub_name):
    """
    Receives messages from a pull subscription.
    """
    #-- appply UnavailableIgnorePolicy to skip UNAVAILABLE
    subscriber = pubsub_v1.SubscriberClient(policy_class = UnavailableIgnorePolicy)
    subscription_path = subscriber.subscription_path(project, sub_name)

    scb = sub_callback(sub_name)
    subscriber.subscribe(subscription_path, callback=scb.callback)

    return {'client':subscriber, 'path':subscription_path}


#TODO g_lock
g_taskInstDict_lock = threading.RLock() 
g_taskInstDict = {}
class sub_callback() : 
    def __init__(self, sub_name) :
        self.sub_name = sub_name
 
    def callback(self, msg) :
	for mod_name, py_mod in g_pluginMods.iteritems():
	    try:
		#-- get class
		taskCls = getattr(py_mod, mod_name)

		#-- instantiate the object of plugin task class 
		if issubclass(taskCls, Task.Task):
                    msg = HMessage(msg)
                    k = '{}/{}/{}'.format(self.sub_name, msg.get_eventType(), msg.get_codename())
                    print k
                    with g_taskInstDict_lock:
                        if not k in g_taskInstDict:
                           taskInst = taskCls(msg)
                           taskInst.start()
                           g_taskInstDict[k] = taskInst 
#TODO gap check
                        interval = taskCls.INVOKE_INTERVAL_SEC

	    except Exception as e:
		logger.error(e, exc_info=True)

	msg.ack()


def receive_messages(project, sub_name):
    sub = create_subscriber(project, sub_name)
    logger.info('Listening for messages on %s', sub['path'])



if '__main__' == __name__ :
    receive_messages('venraasitri', 'pull_bucket_ven-custs')

    #-- The subscriber is non-blocking, so we must keep the main thread from
    #   exiting to allow it to process messages in the background.
    try:
        while True:
            load_plugin_modules()

            time.sleep(3)
    except KeyboardInterrupt:
        print "shutdown requested, exiting... "

    print 'end.'

