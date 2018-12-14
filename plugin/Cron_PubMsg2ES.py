import time
import logging
import json
import requests
import plugin.Task as Task
from lib.hmessage import HMessage
from lib.event import EnumEvent
from lib.topic import EnumTopic

logger = logging.getLogger(__name__)


class Cron_PubMsg2ES(Task.Task):
    INVOKE_INTERVAL_SEC = 30
    LISTEN_SUBSCRIPTS = []
    LISTEN_EVENTS = []
    PUB_TOPIC = EnumTopic['es-cluster']

    LB_ES_HOSTS = 'es-node-01'
    URL_VENRAAS_COMS = 'http://{h}:9200/venraas/_search?sort=update_dt:desc&size=1'


    def __init__(self, interval_sec=300):
        super(Cron_PubMsg2ES, self).__init__()

        self.interval_sec = interval_sec
        self.start_time = time.time()

    def exe(self):
        while True:
            elapsed_time = time.time() - self.start_time;

            if self.interval_sec <= elapsed_time:
                logger.info('{:.2f} < {} elapaed seconds'.format(self.interval_sec, elapsed_time))
                logger.info('publish messages...')

                url = Cron_PubMsg2ES.URL_VENRAAS_COMS.format(h=Cron_PubMsg2ES.LB_ES_HOSTS)
                resp = requests.get(url)
                raasJson = json.loads(resp.text)
                coms  = raasJson['hits']['hits'][0]['_source']['companies']
                for com in coms:
                    if not com['enabled'] and \
                       not com['code_name']:
                        continue

                    codename = com['code_name']

                    #-- publish message of uploaded data
                    hmsg = HMessage()
                    hmsg.set_codename(codename)
                    hmsg.set_eventType(EnumEvent.CRON_SCHEDULER)
                    hmsg.set_objectIds([])
                    logger.info(hmsg)

                    self.pub_message(self.PUB_TOPIC, [hmsg])

                break
            else:
                logger.info('{:.2f} elapaed seconds < {}'.format(elapsed_time, self.interval_sec))

            time.sleep(60)
