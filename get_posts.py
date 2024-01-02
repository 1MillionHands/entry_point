import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from onemilshared.connectors.s3_connector import S3Connector
from onemilshared.connectors.sqs_connector import SQSConnector
from schedule import every, repeat, run_pending
from time import sleep
import uuid
import datetime
from DB_Manager_EP.DB_alchemy import *
from DB_Manager_EP.utils import *

SCRAPING_POSTS = "scraping_posts"
POSTS_NUM = 50

with open('config_file.json', 'r') as f:
    config_data = json.load(f)

headers = {"accept": "application/json",
           "Cookie": f"CF_Authorization={config_data['letBotsWorkToekn']}"}
page_size = config_data.get("page_size")

retry_strategy = Retry(
    total=10,  # Maximum number of retries
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)
adapter = HTTPAdapter(max_retries=retry_strategy)

session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)


def create_messages(post_id_values_to_update, post_history_id_values_to_update):
    messages = []
    for i in range(0, len(post_history_id_values_to_update), POSTS_NUM):
        from_i = i
        to_i = i + POSTS_NUM
        # Ensure the range does not exceed the length of post_id_values_to_update
        if to_i <= len(post_id_values_to_update):
            post_id_slice = post_id_values_to_update[from_i:to_i]
            post_history_id_slice = post_history_id_values_to_update[from_i:to_i]
            # Now you can use post_id_slice and post_history_id_slice
            message = {"posts_id_list": post_id_slice, "posts_history_id_list": post_history_id_slice}
            messages.append(message)
        elif to_i > len(post_id_values_to_update) >= from_i:
            to_i_posts = min(i + POSTS_NUM, len(post_id_values_to_update))
            to_i_hist = to_i
            post_id_slice = post_id_values_to_update[from_i:to_i_posts]
            post_history_id_slice = post_history_id_values_to_update[from_i:to_i_hist]
            message = {"posts_id_list": post_id_slice, "posts_history_id_list": post_history_id_slice}
            messages.append(message)
        else:
            # Handle the case where the range exceeds the length of post_id_values_to_update
            to_i = min(i + POSTS_NUM, len(post_history_id_values_to_update))
            message = {"posts_history_id_list": post_history_id_values_to_update[from_i:to_i]}
            messages.append(message)

    return messages


def send_messages(post_id_values_to_update, post_history_id_values_to_update, q):
    messages = create_messages(post_id_values_to_update, post_history_id_values_to_update)
    for m in messages:
        m = json.dumps(m)
        response = q.send_message(m)
        print(f"Sent Message: {response['MessageId']}")


@repeat(every(config_data['schedule']['hours']).hours)
def get_posts():
    response = session.get(f'https://api.oct7.io/posts?sort=created_at.desc&limit={page_size}', headers=headers,
                           timeout=50)

    if response.status_code == 200:
        raw_posts = response.text

        p = json.loads(raw_posts)['results']

        # upload to db
        p = match_db_field_names(p)
        p = preprocess_posts_to_fit_db(p)
        post_id_values_to_update, post_history_id_values_to_update = insert_posts_and_creators_ep(p)
        sqs = SQSConnector(config_data['sqs']['queue_url'], access_key=config_data['sqs']['access_key'],
                           secret_key=config_data['sqs']['secret_key'])
        send_messages(post_id_values_to_update, post_history_id_values_to_update, sqs)

        # test
        # Receiving messages
        messages = sqs.receive_messages(num_messages=1, visibility_timeout=30)
        for message in messages:
            print(f"Received Message: {message['Body']}")
            # Process the message

            # Deleting the processed message
            sqs.delete_message(message['ReceiptHandle'])
            print(f"Deleted Message: {message['MessageId']}")


        # upload to s3
        s3 = S3Connector(access_key=config_data['s3']['access_key'], secret_key=config_data['s3']['secret_key'],
                         input_file='raw_posts.json')
        s3.write_raw_posts(raw_posts)

        print("SUCCESS")
    else:
        print("FAILED")


get_posts()

while True:
    print(f"initating scheduler every {config_data['schedule']['hours']} hours")
    run_pending()
    sleep(5)
