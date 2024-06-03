import json


HISTORY_IDS = "posts_history_id_list"
POSTS_NUM = 50
POST_IDS = "posts_id_list"


with open('DB_Manager_EP/config_file.json', 'r') as f:
    config_data = json.load(f)

class EventHandlerUtils:
    EVENT_NAME = 'event_name'

    SUCCESS_RESPONSE = 200

    SUCCESS = "SUCCESS"

    page_size = 500

class PostUtils:
    POST_FK = 'post_fk'

    POST_HISTORY_ID = 'post_history_id'

    POST_ID = 'post_id'

    CREATOR_HISTORY_ID = 'creator_history_id'

    CREATOR_ID = 'creator_id'

    # * the name are after preprocess
    POST_HISTORY_VARIABLES = ['num_of_likes', 'num_of_views', 'timestamp', 'scrape_status', 'curr_engagement',
                              'num_of_comments', 'video_play_count', 'video_view_count', 'virality_score', 'post_fk',
                              'post_history_id']
    POST_VARIABLES = ['url', 'publish_date', 'location', 'content', 'type', 'media_url', 'image_url', 'sentiment',
                      'post_id', 'creator_fk', 'platform_type', 'virality_score','hashtag_list']
    CREATOR_HISTORY_VARIABLES = ['owner_post_count', 'owner_follower_count', 'creator_fk', 'creator_history_id']
    CREATOR_FIELDS = ['creator_image', 'name', 'creator_url', 'is_verified', 'creator_id', 'platform_type']

    SUCCESS_RESPONSE = 200

    SUCCESS = "SUCCESS"

    platform_mapping = {
        'linkedin': 'LINKEDIN',
        'Linkedin': 'LINKEDIN',
        'instagram': 'INSTAGRAM',  # Typo corrected to 'instagram'
        'Instagram': 'INSTAGRAM',
        'youtube': 'YOUTUBE',
        'Youtube': 'YOUTUBE',
        'twitter': 'TWITTER',
        'Twitter': 'TWITTER',
        'facebook': 'FACEBOOK',
        'Facebook': 'FACEBOOK',
        'tiktok': 'TIKTOK',
        'Tiktok': 'TIKTOK'
    }