import time
import base64
import requests
import logging


logger = logging.getLogger(__name__)

PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
NUM_RETRIES = 3


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)

def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)

def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return fqrn('topics', project, topic)

def get_projectID():
    r = requests.get('http://metadata.google.internal/computeMetadata/v1/project/project-id', headers={'Metadata-Flavor':'Google'})
    return r.text

def pull_messages(client, sub_name, callback_fun):
    proj_name = get_projectID()

    subscription = get_full_subscription_name(proj_name, sub_name)
    
    try:
        logger.info("pull from {} ...".format(subscription))

        body = {
            'returnImmediately': False,
            'maxMessages': 1
        }
        resp = client.projects().subscriptions().pull(
                subscription=subscription, body=body).execute(
                num_retries=NUM_RETRIES)

        receivedMessages = resp.get('receivedMessages')
        logger.info(receivedMessages)
        if receivedMessages:
            ack_ids = []
            for msg in receivedMessages:
                ack_ids.append(msg.get('ackId'))
                
                message = msg.get('message')
                if message:
                    callback_fun(message)

            ack_body = {'ackIds': ack_ids}
            logger.info(ack_body)
            client.projects().subscriptions().acknowledge(
                    subscription=subscription, body=ack_body).execute(num_retries=NUM_RETRIES)
    except Exception as e:
        time.sleep(0.5)
        logger.error(e, exc_info=True)
    except KeyboardInterrupt:
        logger.info("shutdown requested, exiting... ")


def publish_message(client, topic_enum, rawMsgs):
    ##-- Publish a message to a given topic.
    try:
        proj_name = get_projectID()

        topic = get_full_topic_name(proj_name, topic_enum.name)

        #-- concate message list
        #   note that the client.projects().subscriptions().pull(...) gets message seperately, e.g. len(receivedMessages) = 1, even if 1 < len(rawMsgs)
        pubMsgs = []
        for msg in rawMsgs:
            attributes = msg.get('attributes')
            data = base64.b64encode( str(msg.get('data')) ) if 'data' in msg else ''
            pubMsgs.append(
                {
                    'attributes': attributes, 
                    'data': data
                }
            )
        
        body = {'messages': pubMsgs}

        resp = client.projects().topics().publish(
            topic=topic, body=body).execute(num_retries=NUM_RETRIES)

        logger.info('Published a message "{}" to a topic {}. The message_id was {}.'.format(body, topic, resp.get('messageIds')[0]))
    except Exception as e:
        logger.error(e, exc_info=True)
