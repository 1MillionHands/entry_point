from datetime import datetime
# import pandas as pd

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
