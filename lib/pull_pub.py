import time


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def get_full_subscription_name(project, subscription):
    """Return a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)


def pull_messages(client, proj_name, sub_name, callback_fun):
    NUM_RETRIES = 3

    subscription = get_full_subscription_name(proj_name, sub_name)
    
    try:
        print "pull from {} ...".format(subscription)

        body = {
            'returnImmediately': False,
            'maxMessages': 1
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
            print ack_body
            client.projects().subscriptions().acknowledge(
                    subscription=subscription, body=ack_body).execute(num_retries=NUM_RETRIES)
    except Exception as e:
        time.sleep(0.5)
        print e
    except KeyboardInterrupt:
        print "shutdown requested, exiting... "
