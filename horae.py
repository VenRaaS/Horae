###
# Horae - A plug-able event driven task execution framework.
# Google Cloud Pub/Sub API applied.
# see https://developers.google.com/api-client-library/
###

import time
import threading
import logging
import os
import imp
import sys
from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

from lib.hmessage import HMessage
from lib.subscr import EnumSubscript
from lib.event import EnumEvent
from lib.tstatus import EnumState
import lib.pull_pub as pull_pub
import plugin.Task

#-- logging setup
#   see https://docs.python.org/2/howto/logging.html#configuring-logging
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
                logger.info('%s, %s is inherited from Task.Task: %s', eventType, taskClass, hasattr(taskClass, 'isTask'))
                if not hasattr(taskClass, 'isTask'): continue
                
                #-- math Subscription and eventType between message and plugin class (taskClass)
                if not self.subscript in taskClass.LISTEN_SUBSCRIPTS: continue
                if not eventType or not EnumEvent[eventType] in taskClass.LISTEN_EVENTS: continue

                k = '{}/{}/{}/{}'.format(self.subscript.name, eventType, hMsg.get_codename(), mod_name)
                logger.info('task instance key: %s', k)

                with g_taskInstDict_lock:
                    if not k in g_taskInstDict or EnumState.END == g_taskInstDict[k].st.state:
                        #-- instantiate the object of plugin task class 
                        taskInst = taskClass(hMsg)
                        g_taskInstDict[k] = taskInst
                        taskInst.start()
                    else:
                        logger.info('skip message due to task: %s is not expired yet, %s', k, hMsg.get_attributes())

            except Exception as e:
                logger.error(e, exc_info=True)


if '__main__' == __name__ :
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(pull_pub.PUBSUB_SCOPES)
    client = discovery.build('pubsub', 'v1', credentials=credentials)

    
    #-- The subscriber is non-blocking, so we must keep the main thread alive
    #   and process messages in the background.
    try:
        while True:
            load_plugin_modules()
            with g_taskInstDict_lock:
                keys = g_taskInstDict.keys()
                for k in keys:
                    taskInst = g_taskInstDict[k]
                    if EnumState.END == taskInst.st.state:
                        if taskInst.INVOKE_INTERVAL_SEC < taskInst.st.elapsed_afterend_sec():
                            del g_taskInstDict[k]
                            logger.info('del %s', k)

            cb = sub_callback(EnumSubscript['pull_bucket_ven-custs'])
            pull_pub.pull_messages(client, EnumSubscript['pull_bucket_ven-custs'].name, cb.callback)
            pull_pub.pull_messages(client, EnumSubscript['pull_bigquery'].name, cb.callback)

            time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info('shutdown requested, exiting... ')

    logger.info('end.')


