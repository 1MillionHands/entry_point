import json
from onemilshared.connectors.sqs_connector import SQSConnector
from datetime import datetime
# import pandas as pd


HISTORY_IDS = "posts_history_id_list"
POSTS_NUM = 50
POST_IDS = "posts_id_list"
platform_mapping = {
    'linkedin': 'LINKEDIN',
    'Linkedin': 'LINKEDIN',
    'instagram': 'INSTGRAM',  # Typo corrected to 'instagram'
    'Instagram': 'INSTGRAM',
    'youtube': 'YOUTUBE',
    'Youtube': 'YOUTUBE',
    'twitter': 'TWITTER',
    'Twitter': 'TWITTER',
    'facebook': 'FACEBOOK',
    'Facebook': 'FACEBOOK',
    'tiktok': 'TIKTOK',
    'Tiktok': 'TIKTOK'
}
with open('config_file.json', 'r') as f:
    config_data = json.load(f)


# def match_db_field_names(df):
#     # Define a mapping of old column names to new column names
#     column_names_mapping = {
#         'created_at': 'publish_date',
#         'number_of_comments': 'num_of_comments',
#         'number_of_likes': 'num_of_likes',
#         'post_url': 'url',
#         'pro_israel': 'sentiment',
#         'last_scrape_time': 'timestamp',
#         'post_type': 'type',
#         'city': 'location',
#         'creator_name': 'name',
#         'is_influencer': 'is_verified',
#         'platform': 'platform_type',
#     }
#
#     df.rename(columns=column_names_mapping, inplace=True)

def match_db_field_names(data):
    # Define a mapping of old column names to new column names
    column_names_mapping = {
        'created_at': 'publish_date',
        'comment_count': 'num_of_comments',
        'like_count': 'num_of_likes',
        'post_url': 'url',
        'pro_israel': 'sentiment',
        'last_scrape_time': 'timestamp',
        'post_type': 'type',
        'city': 'location',
        'platform_avatar_username': 'name',
        'is_influencer': 'is_verified',
        'platform': 'platform_type',
        'platform_text': 'content',
        'platform_avatar_pic_url': 'creator_image',
    }

    for entry in data:
        for old_name, new_name in column_names_mapping.items():
            if old_name in entry:
                entry[new_name] = entry.pop(old_name)

    return data




def preprocess_posts_to_fit_db(posts):
    max_length = 255
    for post in posts:
        # Replace None, 'nan', or empty strings with default values or None
        for key, value in post.items():
            if post[key] == 'None' or str(post[key]).lower() == 'nan' or post[key] == "":
                post[key] = None
            if (key == 'media_url' or key == 'creator_image') and isinstance(value, str) and len(
                    value) > max_length:
                post[key] = value[:max_length]
            elif key == 'platform_type':
                post[key] = platform_mapping.get(post[key])
            if key == 'curr_engagement' and value:
                try:
                    post[key] = int(float(value))
                except:
                    # Handle the case where the value cannot be converted to float or int
                    post[key] = 0

        if post['sentiment'] == False:
            post['sentiment'] = '-1'
        else:
            post['sentiment'] = '1'

        # # Convert the content_publish_date to a Python datetime object
        # content_publish_date = post['content_publish_date']
        # if content_publish_date:
        #     post['content_publish_date'] = post.strptime(content_publish_date, '%Y-%m-%d %H:%M:%S')
        #
        # content_time_stamp = post['content_time_stamp']
        # if content_time_stamp:
        #     post['content_time_stamp'] = datetime.strptime(content_time_stamp, '%Y-%m-%d %H:%M:%S')

    return posts


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