import base64
import pprint
import json
from datetime import date

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import lib.utility as util


PUBSUB_SCOPES = ["https://www.googleapis.com/auth/pubsub"]
NUM_RETRIES = 3
BATCH_SIZE = 10


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)

def get_full_topic_name(project, topic):
    """Return a fully qualified topic name."""
    return fqrn('topics', project, topic)

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


if __name__ == '__main__':
    TOPIC = 'es-cluster'
    CODENAME = 'crazymike'
    INDEX = 'opp'
    
    today = date.today().strftime('%Y%m%d')
    index_es = '{cn}_{idx}_{d}'.format(cn=CODENAME, idx=INDEX, d=today)
    msg = {
            'attributes': 
            {
                'objectIds': json.dumps( [index_es] ),
                'eventType': 'OBJECT_FINALIZE',
                'codename': 'crazymike',
                
            },
            'data':'ooxx...'
    }

    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    client = discovery.build('pubsub', 'v1', credentials=credentials)
    publish_message(client, util.get_projectID(), TOPIC, msg)



