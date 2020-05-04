

## Overview
In venraas, we leverage [GCP Pub/Sub](https://cloud.google.com/pubsub/docs/overview) and make a service, Horae, which contains multiple plugins for task executions with the predefined event condictions.  
Please check the following materials for more detail.
* [Pub/Sub concepts and message flow](https://cloud.google.com/pubsub/docs/overview#concepts)
* [Subscriber overview](https://cloud.google.com/pubsub/docs/subscriber)

The plugin client is able to trigger a task for data sync or modeling process.  
Here is an overview of the Horae framework and how the interaction between message flow and data flow:

![](https://github.com/VenRaaS/Horae/blob/master/doc/img/horae_overview.PNG?raw=true)

## Setup
* Installation of python packages (check [pip](https://www.tecmint.com/install-pip-in-linux/) if pip isn't available in your os)  
  `pip install -r requirements`
  
* [Applying notification to a bucket with gsutil](https://github.com/VenRaaS/Horae/wiki/Sync-GOCC-to-BigQuery-and-ES-via-Pub-Sub-Notifications-with-Cloud-Storage#applying-notification-to-a-bucket-with-gsutil)


## [Topics and Subscriptions](https://cloud.google.com/pubsub/docs/admin)
* [topic.py](https://github.com/VenRaaS/Horae/blob/master/lib/topic.py)
* [subscr.py](https://github.com/VenRaaS/Horae/blob/master/lib/subscr.py)

## Topics and plugins process order
* The Red line stands for GOCC upload/update message and task processing flow (up to bottom).  
* The Green line stands for key alias (date) processing flow (up to bottom).  
  * `Cron_PubMsg2MS.py` always be invoked regularly and publics a cron message to `ms-cluster`, and then the `MSAlterIndexAliases.py` will be instantiated and try to alter the key alias with the latest date. 

![](https://github.com/VenRaaS/Horae/blob/master/doc/img/topic_and_plugins.PNG?raw=true)

## [Message](https://github.com/VenRaaS/Horae/wiki/Message-format)
```
"message":{  
    "attributes":{  
        "bucketId":"ven-cust-sohappy",
        "eventType":"OBJECT_FINALIZE",
        "objectId":"tmp/sohappy_gocc_20170231.tar.gz",
...
```

## sub_callback and plugin modules
TODO ...

## Reference
* [Google API Client Libraries](https://github.com/google/google-api-python-client)
  * [Google API Client Libraries - Python](https://developers.google.com/api-client-library/python/)
  * [APIs Explorer - Google Cloud Pub/Sub API v1](https://developers.google.com/apis-explorer/#p/pubsub/v1/)
  * [cloud-pubsub-samples-python](https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/blob/master/cmdline-pull/pubsub_sample.py)
* [Cloud Pub/Sub Client Libraries](https://cloud.google.com/pubsub/docs/reference/libraries#client-libraries-install-python)
  * [PubSub python client returning StatusCode.UNAVAILABLE](https://stackoverflow.com/questions/46788681/google-pubsub-python-client-returning-statuscode-unavailable)
  * [PubSub Subscriber does not catch & retry UNAVAILABLE errors](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4234)
  * [PubSub Subscriber: CPU Usage eventually spikes to 100%](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4600)
  
