
class HMessage() :
    def __init__(self, ps_msg) :
        self.msg = ps_msg if ps_msg else {}

    def get_codename(self) :
        codename = ''
        attr = self.msg.get('attributes')

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

    def get_attributes(self) :
        return self.msg.get('attributes')

    def get_eventType(self) :
        attr = self.msg.get('attributes')
        return attr['eventType'] if attr and 'eventType' in attr else ''

    def __str__(self) :

        return str(self.msg)
