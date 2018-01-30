

## Overview
![](https://drive.google.com/uc?id=1qnS-pLb7ZfK__745vSx9J5bdidx6b8Tw)


## Topics
* bucket_ven-custs
* bigquery
* es-cluster

## [Message](https://github.com/VenRaaS/Horae/wiki/Message-format)
```
"message":{  
    "attributes":{  
        "bucketId":"ven-cust-sohappy",
        "eventType":"OBJECT_FINALIZE",
        "objectId":"tmp/sohappy_gocc_20171121.tar.gz",
...        
```


## Reference
* [Cloud Pub/Sub Client Libraries](https://cloud.google.com/pubsub/docs/reference/libraries#client-libraries-install-python)
  * [PubSub python client returning StatusCode.UNAVAILABLE](https://stackoverflow.com/questions/46788681/google-pubsub-python-client-returning-statuscode-unavailable)
  * [PubSub Subscriber does not catch & retry UNAVAILABLE errors](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4234)
  * [PubSub Subscriber: CPU Usage eventually spikes to 100%](https://github.com/GoogleCloudPlatform/google-cloud-python/issues/4600)
* [Google API Client Libraries](https://developers.google.com/api-client-library/) - Google Cloud Pub/Sub API
  * [APIs Explorer - Google Cloud Pub/Sub API v1](https://developers.google.com/apis-explorer/#p/pubsub/v1/)
  * [cloud-pubsub-samples-python](https://github.com/GoogleCloudPlatform/cloud-pubsub-samples-python/blob/master/cmdline-pull/pubsub_sample.py)
