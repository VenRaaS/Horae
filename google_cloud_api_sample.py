import base64
import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
NUM_RETRIES = 3
BATCH_SIZE = 10

PROJECT_NAME = 'venraasitri'
TOPIC = 'bucket_ven-custs'



def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)

def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return fqrn('topics', project, topic)

def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)

def publish_message(client, proj_name, topic, msg):
    """Publish a message to a given topic."""
    try:
        topic = get_full_topic_name(proj_name, topic)

        attributes = msg['attributes']
        data = base64.b64encode(str(msg['data']))
        body = {'messages': [
                    {
                        'attributes': attributes, 
                        'data': data
                    }
                ]}

        resp = client.projects().topics().publish(
            topic=topic, body=body).execute(num_retries=NUM_RETRIES)

        print ('Published a message "{}" to a topic {}. The message_id was {}.'
               .format(msg['attributes'], topic, resp.get('messageIds')[0]))
    except Exception as e:
        print e

def pull_messages(client, proj_name, sub_name, callback_fun):
    subscription = get_full_subscription_name(PROJECT_NAME, sub_name)
    
    while True:
        try:
            print "pull..."

            body = {
                'returnImmediately': False,
                'maxMessages': BATCH_SIZE
            }
            resp = client.projects().subscriptions().pull(
                    subscription=subscription, body=body).execute(
                    num_retries=NUM_RETRIES)

            receivedMessages = resp.get('receivedMessages')
            if receivedMessages:
                ack_ids = []
                for msg in receivedMessages:
                    ack_ids.append(msg.get('ackId'))
                    
                    message = msg.get('message')
                    if message:
                        callback_fun(message)

                ack_body = {'ackIds': ack_ids}
                client.projects().subscriptions().acknowledge(
                        subscription=subscription, body=ack_body).execute(num_retries=NUM_RETRIES)
        except Exception as e:
            time.sleep(0.5)
            print e
        except KeyboardInterrupt:
            print "shutdown requested, exiting... "
            break
        


if __name__ == '__main__':
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)

    client = discovery.build('pubsub', 'v1', credentials=credentials)

    def pprint_attr(message):
        pp = pprint.PrettyPrinter(indent=4)

        attributes = message.get('attributes')
        if attributes:
            pp.pprint(attributes)
            print base64.b64decode(str(message.get('data')))
    

    pull_messages(client, PROJECT_NAME, 'pull_bucket_ven-custs', pprint_attr)

    msg = {
            'attributes': 
            {
                'bucketId': 'ven-cust-hohappy',
                'eventType': 'OBJECT_FINALIZE'
            },
            'data':'ewogICJraW5kIjogInN0b3JhZ2Ujb2JqZWN0...'
    }
#    publish_message(client, PROJECT_NAME, TOPIC, msg)

