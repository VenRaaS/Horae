import time
import base64
import requests
import logging

import lib.utility as util
from lib.hmessage import HMessage
from lib.event import EnumEvent

###logger = logging.getLogger(__name__)

PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
NUM_RETRIES = 3


def pull_messages(client, sub_enum, callback_fn):
    proj_name = util.get_projectID()

    subscription = util.get_full_subscription_name(proj_name, sub_enum.name)
    
    try:
        logging.info("pull from {} ...".format(subscription))

        #--  pulls messages from the server
        #    see https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/pull
        body = {
            'returnImmediately': True,
            'maxMessages': 1
        }
        resp = client.projects().subscriptions().pull(
                subscription=subscription, body=body).execute(
                num_retries=NUM_RETRIES)

        receivedMessages = resp.get('receivedMessages')
#        logging.info(receivedMessages)
        if receivedMessages:
            ack_ids = []
            for msg in receivedMessages:
                ack_ids.append(msg.get('ackId'))
                
                message = msg.get('message')
                if message:
#                    pass
                    callback_fn(message)

            ack_body = {'ackIds': ack_ids}
            logging.info(ack_body)
            client.projects().subscriptions().acknowledge(
                    subscription=subscription, body=ack_body).execute(num_retries=NUM_RETRIES)
    except Exception as e:
        logging.error(e, exc_info=True)
        time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("shutdown requested, exiting... ")


##
## Publish a message to a given topic.
##
def publish_message(client, topic_enum, hmsgs):
    try:
        proj_name = util.get_projectID()
        topic = util.get_full_topic_name(proj_name, topic_enum.name)
        logging.info('topic: {}'.format(topic))

        #-- concate message list
        #   note that the client.projects().subscriptions().pull(...) gets message seperately, e.g. len(receivedMessages) = 1, even if 1 < len(hmsgs)
        pubMsgs = []
        for m in hmsgs:
            attributes = m.get_attributes()
            data = base64.b64encode( str(m.get_data()) ) if m.get_data() else ''
            pubMsgs.append(
                {
                    'attributes': attributes, 
                    'data': data
                }
            )
        
        body = {'messages': pubMsgs}

        resp = client.projects().topics().publish(topic=topic, body=body).execute(num_retries=NUM_RETRIES)

        logging.info('Published a message "{}" to a topic {}. The message_id was {}.'.format(body, topic, resp.get('messageIds')[0]))
    except Exception as e:
        logging.error(e, exc_info=True)

 
if '__main__' == __name__ :
    msgObjs = ['titantech_unima.category_20180310', 'titantech_unima.goods_20180310', 'titantech_unima.goodscatecode_20180310'] 
    hmsg = HMessage()
    hmsg.set_codename('gohappy')
    hmsg.set_eventType(EnumEvent.OBJECT_FINALIZE)
    hmsg.set_objectIds(msgObjs)
    logging.info(hmsg)
    pull_pub.publish_message(client, EnumTopic.bigquery, [hmsg])
    
    msgObjs = ['titantech_gocc_20180309']
    hmsg = HMessage()
    hmsg.set_codename('titantech')
    hmsg.set_eventType(EnumEvent.OBJECT_FINALIZE)
    hmsg.set_objectIds(msgObjs)
    logging.info(hmsg)
    pull_pub.publish_message(client, EnumTopic['es-cluster'], [hmsg])
