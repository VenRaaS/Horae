

## Overview
In venraas, we leverage [GCP Pub/Sub](https://cloud.google.com/pubsub/docs/overview) to pass the message/status between modules.  
The plugin client is able to trigger a task for data sync or modeling process.  
Here is an overview of the Horae framework and how the interaction between message flow and data flow:

![](https://drive.google.com/uc?id=1qnS-pLb7ZfK__745vSx9J5bdidx6b8Tw)

## Setup
* [Applying notification to a bucket with gsutil](https://github.com/VenRaaS/Horae/wiki/Sync-GOCC-to-BigQuery-and-ES-via-Pub-Sub-Notifications-with-Cloud-Storage#applying-notification-to-a-bucket-with-gsutil)


## [Topics](https://cloud.google.com/pubsub/docs/admin#managing_topics)
* [bucket_ven-custs](https://github.com/VenRaaS/Horae/blob/master/lib/topic.py#L8)
* [bigquery](https://github.com/VenRaaS/Horae/blob/master/lib/topic.py#L8)
* [es-cluster](https://github.com/VenRaaS/Horae/blob/master/lib/topic.py#L8)

## [Message](https://github.com/VenRaaS/Horae/wiki/Message-format)
```
"message":{  
    "attributes":{  
        "bucketId":"ven-cust-sohappy",
        "eventType":"OBJECT_FINALIZE",
        "objectId":"tmp/sohappy_gocc_20170231.tar.gz",
...        
```


## Reference
* [Google API Client Libraries](https://github.com/google/google-api-python-client)
  * [Google API Client Libraries - Python](https://developers.google.com/api-client-library/python/)
  * [APIs Explorer - Google Cloud Pub/Sub API v1](https://developers.google.com/apis-explorer/#p/pubsub/v1/)
  * [cloud-pubsub-samples-python](https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/blob/master/cmdline-pull/pubsub_sample.py)
* [Cloud Pub/Sub Client Libraries](https://cloud.google.com/pubsub/docs/reference/libraries#client-libraries-install-python)
  * [PubSub python client returning StatusCode.UNAVAILABLE](https://stackoverflow.com/questions/46788681/google-pubsub-python-client-returning-statuscode-unavailable)
  * [PubSub Subscriber does not catch & retry UNAVAILABLE errors](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4234)
  * [PubSub Subscriber: CPU Usage eventually spikes to 100%](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4600)
  
