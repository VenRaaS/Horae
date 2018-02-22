import json

from lib.event import EnumEvent


class HMessage() :
    def __init__(self, ps_msg = None) :
        self.msg = ps_msg if ps_msg else {}

    def get_codename(self) :
        codename = ''

        attr = self.get_attributes()
        if attr and 'codename' in attr:
            codename = attr['codename']
        elif attr and 'eventType' in attr:
            event_type = attr['eventType']

            if event_type and 'OBJECT_FINALIZE' == event_type:
                bucket_id = attr.get('bucketId') if 'bucketId' in attr else ''
                if bucket_id:
                    codename = bucket_id.split('-')[-1]
                    attr['codename'] = codename

        return codename

    def set_codename(self, codename) :
       self.set_attributes()
       self.msg['attributes']['codename'] = codename

    def get_attributes(self) :
        return self.msg.get('attributes')

    def set_attributes(self) :
        if not self.get_attributes():
            self.msg['attributes'] = {}

    def get_data(self) :
        return self.msg.get('data')

    def get_eventType(self) :
        attr = self.get_attributes()
        #TODO - get type of EnumEvent not string
        return attr['eventType'] if attr and 'eventType' in attr else ''

    def set_eventType(self, enumEvent) :
        self.set_attributes()
        if type(EnumEvent.DEFAULT) == type(enumEvent):
            self.msg['attributes']['eventType'] = enumEvent.name 

    def get_objectId(self) :
        attr = self.get_attributes()
        return attr['objectId'] if attr and 'objectId' in attr else ''

    def set_objectId(self, oId) :
        self.set_attributes()
        self.msg['attributes']['objectId'] = oId

    def get_objectIds(self) :
        attr = self.get_attributes()
        l = []
        if attr and 'objectIds' in attr:
            l = json.loads(attr['objectIds'])
        return l
    
    def set_objectIds(self, objs) :
        self.set_attributes()
        l = objs if type([]) == type(objs) else []
        self.msg['attributes']['objectIds'] = json.dumps(l)


    def __str__(self) :
        return str(self.msg)
