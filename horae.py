###
# Horae - A plug-able event driven task execution framework.
# Google Cloud Pub/Sub API applied.
# see https://developers.google.com/api-client-library/
###

import time
import threading
import os
import imp
import sys
import json
import logging
import logging.config
from logging.handlers import RotatingFileHandler
from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

from lib.hmessage import HMessage
from lib.subscr import EnumSubscript
from lib.event import EnumEvent
from lib.tstatus import EnumState
from lib.topic import EnumTopic
import lib.pull_pub as pull_pub
import lib.utility as util
import plugin.Task


def create_topics(client, topics) :
    existing_topics = util.list_all_topics(client)

    for t in topics:
        tp_fullname = util.get_full_topic_name(util.get_projectID(), t)

        if not tp_fullname in existing_topics:
            topic = client.projects().topics().create(
                name=tp_fullname,
                body={}).execute(num_retries=3)
            logger.info('topic {} is created.'.format(tp_fullname))
        else:
            logger.info('topic {} has already existed.'.format(tp_fullname))

def create_subscrs(client, subscrs):
    fts = util.list_all_topics(client)
    fss = util.list_all_subscrs(client)

    for s in subscrs:
        ftn = None
        for ft in fts:
            t = os.path.split(ft)[-1]
            if s.endswith(t):
                ftn = ft
                break

        fsn = util.get_full_subscription_name(util.get_projectID(), s)
        if not fsn in fss:
            subscription = client.projects().subscriptions().create(name=fsn, body={'topic': ftn}).execute(num_retries=3)
            logger.info('subscription {} is created.'.format(subscription['name']))
        else:
            logger.info('subscription {} has already existed.'.format(fsn))

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
                if '__init__' == mod_name:
                    continue

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
                        logger.info('{} is instancing and starting'.format(taskInst))

                        taskInst.start()
                    else:
                        logger.info('skip message due to task: %s is not expired yet, %s', k, hMsg.get_attributes())

            except Exception as e:
                logger.error(e, exc_info=True)

def main() :
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

    #-- google API client
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(pull_pub.PUBSUB_SCOPES)
    client = discovery.build('pubsub', 'v1', credentials=credentials)

    create_topics(client, [t.name for t in list(EnumTopic)])
    create_subscrs(client, [s.name for s in list(EnumSubscript)])

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

            cb_bk = sub_callback(EnumSubscript['pull_bucket_ven-custs'])
            cb_bq = sub_callback(EnumSubscript['pull_bigquery'])
            cb_es = sub_callback(EnumSubscript['pull_es-cluster'])
            pull_pub.pull_messages(client, EnumSubscript['pull_bucket_ven-custs'], cb_bk.callback)
            pull_pub.pull_messages(client, EnumSubscript['pull_bigquery'], cb_bq.callback)
            pull_pub.pull_messages(client, EnumSubscript['pull_es-cluster'], cb_es.callback)

            time.sleep(1)
                
    except KeyboardInterrupt:
        logger.info('shutdown requested, exiting... ')

    logger.info('end.')


if '__main__' == __name__ :
    pwd_dir = os.path.dirname(os.path.realpath(__file__))
    log_dir = os.path.join(pwd_dir, 'log')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    #-- logging setup
    #   see https://docs.python.org/2/howto/logging.html#configuring-logging
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger(__name__)

    main()


