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
from PostAPI import PostAPI
from LetBotWorks import LetBotWork

HISTORY_IDS = "posts_history_id_list"

POST_IDS = "posts_id_list"

SUCCESS_RESPONSE = 200

SUCCESS = "SUCCESS"

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


class EntryPoint(object):

    def __init__(self, post_api: PostAPI):
        self.postApi = post_api

    @staticmethod
    def upload_to_db(posts):
        posts = match_db_field_names(posts)
        posts = preprocess_posts_to_fit_db(posts)
        post_id_values_to_update, post_history_id_values_to_update = insert_posts_and_creators_ep(posts)
        send_messages(post_id_values_to_update, post_history_id_values_to_update)

    @staticmethod
    def upload_to_s3(raw_posts):
        try:
            s3 = S3Connector(access_key=config_data['s3']['access_key'],
                             secret_key=config_data['s3']['secret_key'],
                             input_file='raw_posts.json')
            s3.write_raw_posts(raw_posts)
        except Exception as e:
            print("FAILED", e)

# @repeat(every(config_data['schedule']['hours']).hours)
def get_posts(ep: EntryPoint):
    url = ep.postApi.get_url()
    response = ep.postApi.get_post_information(url)
    if response.status_code == SUCCESS_RESPONSE:
        raw_posts = response.text
        posts = json.loads(raw_posts)['results']
        ep.upload_to_db(posts)
        ep.upload_to_s3(raw_posts)
        print(SUCCESS)
    else:
        print("FAILED", response.status_code)


def create_messages_old(post_id_values_to_update, post_history_id_values_to_update):
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


def create_messages(post_ids_to_update, history_ids_to_update):
    messages = []
    history_size = len(history_ids_to_update)
    for i in range(0, history_size, POSTS_NUM):
        from_i = i
        to_i = i + POSTS_NUM
        # Ensure the range does not exceed the length of post_id_values_to_update
        post_size = len(post_ids_to_update)
        if to_i > post_size >= from_i:
            to_i = min(i + POSTS_NUM, post_size)
        elif to_i > post_size:
            # Handle the case where the range exceeds the length of post_id_values_to_update
            to_i = min(i + POSTS_NUM, history_size)
            from_i = to_i
        post_id_slice = post_ids_to_update[from_i:to_i]
        post_history_id_slice = history_ids_to_update[from_i:to_i]
        message = get_message(post_history_id_slice, post_id_slice)
        messages.append(message)
    return messages


def get_message(history_ids, posts_ids):
    return {POST_IDS: posts_ids, HISTORY_IDS: history_ids}


def send_messages(post_id_values_to_update, post_history_id_values_to_update):
    q = SQSConnector(config_data['sqs']['queue_url'],
                     access_key=config_data['sqs']['access_key'],
                     secret_key=config_data['sqs']['secret_key'])

    messages = create_messages(post_id_values_to_update, post_history_id_values_to_update)
    for m in messages:
        m = json.dumps(m)
        response = q.send_message(m)
        print(f"Sent Message: {response['MessageId']}")





# A wrapper function to pass the EntryPoint to get_posts
def scheduled_get_posts():
    let_bot = LetBotWork(config_data)
    ep = EntryPoint(let_bot)
    get_posts(ep)



# Initiates the scheduler
def run_scheduler():
    # Schedule the wrapper function instead
    every(config_data['schedule']['hours']).hours.do(scheduled_get_posts)
    print(f"Scheduler initiated every {config_data['schedule']['hours']} hours")

    while True:
        run_pending()
        sleep(5)

if __name__ == '__main__':
    let_bot = LetBotWork(config_data)
    ep = EntryPoint(let_bot)
    get_posts(ep)
    run_scheduler()