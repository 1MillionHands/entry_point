
class PostUtils:
    POSTS_NUM = 500

    POST_HISTORY_ID = 'post_history_id'

    POST_ID = 'post_id'

    INGESTION_TIMESTAMP_FIELD = 'ingestion_timestamp'

     # the name are after preprocess
    POST_HISTORY_VARIABLES = [ 'ingestion_timestamp', 'engagement_score_view',  'post_id', 'post_history_id']
    POST_VARIABLES = ['url', 'publish_date', 'location', 'content', 'type', 'media_url', 'image_url', 'sentiment_score',
                      'post_id', 'creator_id']

    column_fields_from_scooper = [
                                  'parent_url', 'published_ts', 'source_country', 'source_country_code',  'source_region',
                                  'source_city', 'source_latitude', 'source_longitude', 'content', 'post_type', 'media_url',
                                  'image_url', 'sentiment_score', 'creator_id', 'engagement_score_view',
                                  'ingestion_timestamp'
                                  ]

    map_from_field_name_from_scooper = {
        'parent_url': 'url',
        'publish_date': 'publish_date',
        'location': 'location',
        'content': 'content',
        'post_type': 'type',
        'media_url': 'media_url',
        'image_url': 'image_url',
        'sentiment_score': 'sentiment_score',
        'engagement_score_view': 'engagement_score_view',
        'creator_id': 'creator_id'
    }

    SUCCESS_RESPONSE = 200

    SUCCESS = "SUCCESS"

    NEW_CREATORS_INDICATOR = 'new_creators_indicator'

    location_fields = ['source_country', 'source_country_code',  'source_region', 'source_city', 'source_latitude', 'source_longitude']

    TIMESTAMP_PARTITION_ID = 'id'

class CreatorUtils:
    query_raw_data_fields = [
    'creator_id',
    'creator_name',
    'sentiment_score',
    'creator_image',
    'creator_url',
    'language',
    'media_url',
    'ingestion_timestamp'
        ]

    CREATOR_FIELDS = [
        'creator_id',
        'creator_name',
        'creator_image',
        'creator_url',
        'language',
        'is_verified',
        'hashtag_list',
        'engagement_history',
        'last_post_date',
        'platform_name',
    ]
    CREATOR_HISTORY_VARIABLES = ['owner_post_count', 'owner_follower_count', 'creator_id', 'creator_history_id', 'ingestion_timestamp']

    valid_platforms = ['twitter', 'youtube', 'facebook', 'instagram', 'tiktok', 'reddit', 'threads']

    primary_key = ['creator_id', 'platform_name']

    NEW_CREATORS_ID_EXIST = 'new_creators_id_exist'

    CREATOR_HISTORY_ID = 'creator_history_id'

    CREATOR_ID = 'creator_id'

    CREATOR_ID_YEAR = 'creator_id_y'

    INGESTION_TIMESTAMP_FIELD = 'ingestion_timestamp'

    is_numeric = ['', '', '']

    TIMESTAMP_PARTITION_ID = 'id'
