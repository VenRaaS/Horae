
class HMessage() :
    def __init__(self, pb_msg) :
        self.msg = pb_msg

    def get_codename(self) :
        codename = ''

	attr = self.msg.attributes
        if 'codename' in attr:
            codename = attr['codename']
        elif 'eventType' in attr:
	    event_type = attr['eventType']
	    if 'OBJECT_FINALIZE' == event_type:
		bucket_id = attr['bucketId'] if 'bucketId' in attr else ''
                codename = bucket_id.split('-')[-1]
                attr['codename'] = codename

        return codename

    def get_eventType(self) :
	attr = self.msg.attributes
        return attr['eventType'] if 'eventType' in attr else ''

    def __str__(self) :
        return str(self.msg)
