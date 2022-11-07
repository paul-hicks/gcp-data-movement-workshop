import datetime
import json
import os
import random
import uuid

PROJECT = 'dataflow-workshop-demo'

STATES = ['MO', 'SC', 'IN']


def publish_web_traffic_data(topic="web-traffic", project=PROJECT):
    """
    user_id:
    timestamp:
    state_code:
    """
    data = {
        'user_id': str(uuid.uuid4()),
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'state_code': random.choice(STATES)
    }

    message = json.dumps(data)
    command = f"gcloud --project {project} pubsub topics publish {topic} --message='{message}'"
    print(command)
    os.system(command)


def publish_transaction_data(topic="transactions", project=PROJECT):
    """
    state_code [str],
    timestamp [int32],
    item_count [int32] (rand 1-20),
    total_sale_amount [float] (rand 10.00-1000.00)

    """
    data = {
        'state_code': random.choice(STATES),
        'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'item_count': random.randint(3, 9),
        'total_sale_amount': round(random.uniform(10, 1000), 2)
    }

    message = json.dumps(data)
    command = f"gcloud --project {project} pubsub topics publish {topic} --message='{message}'"
    print(command)
    os.system(command)


while True:
    pub_func = random.choice([publish_web_traffic_data, publish_transaction_data])
    pub_func()