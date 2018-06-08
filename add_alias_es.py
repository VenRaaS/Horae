import base64
import pprint
import json
from datetime import date

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from lib.hmessage import HMessage
from lib.topic import EnumTopic
from lib.event import EnumEvent
import lib.pull_pub as pull_pub


if __name__ == '__main__':
    codename = 'crazymike'
    index = 'opp'
    
    today = date.today().strftime('%Y%m%d')
    index_es = '{cn}_{idx}_{d}'.format(cn=codename, idx=index, d=today)
    objIds = [index_es]

    hmsg = HMessage()
    hmsg.set_codename(codename)
    hmsg.set_eventType(EnumEvent.OBJECT_FINALIZE)
    hmsg.set_objectIds(objIds)

    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(PUBSUB_SCOPES)
    client = discovery.build('pubsub', 'v1', credentials=credentials)

    pull_pub.publish_message(client, EnumTopic['es-cluster'], [hmsg])

