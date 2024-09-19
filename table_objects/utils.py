import requests
import boto3
import os
from urllib.parse import urlparse

# Initialize the S3 client
s3 = boto3.client("s3")

# Your S3 bucket name
S3_BUCKET_NAME = 'your-s3-bucket-name'


class ImageDownloader:

    @staticmethod
    def download_image(url):
        """
        Downloads an image from a URL.
        """
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Ensure the request was successful
            return response.content  # Return the raw image content
        except Exception as e:
            print(f"Error downloading image from {url}: {str(e)}")
            return None

    @staticmethod
    def get_image_name(url):
        """
        Extract the image name from the URL. This assumes the URL contains the image name at the end.
        Example: https://example.com/images/image1.jpg -> image1.jpg
        """
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('/')
        return path_parts[-2]

    def run(self,  posts_url_list, s3_obj,bucket_name='omh-media', s3_path_prefix='posts_media'):
        """
        Runs the process to download images and upload to S3.
        """
        total_failed_downloads = 0
        for image_url in posts_url_list: # Assuming your post object/dict has an 'image_url' field
            if image_url:
                # Download the image
                image_data = self.download_image(image_url)

                if image_data:
                    # Get the image name
                    image_name = self.get_image_name(image_url)

                    # Define the S3 path prefix
                    key = f"{s3_path_prefix}/{image_name}.jpg"

                    # Upload the image to S3
                    s3_obj._put_object(object_data=image_data, bucket=bucket_name, key=key)
                else:
                    print(f"No image data found for post: {image_url}")
                    total_failed_downloads += 1

            else:
                print(f"No image URL found for post: {image_url}")

        print(f"Total failed downloads: {total_failed_downloads} out of {len(posts_url_list)}")


if __name__ == "__main__":
    downloader = ImageDownloader()
    downloader.run()


class PostUtils:
    POSTS_NUM = 500

    PLATFORM = 'platform_type'

    CREATOR_NAME = 'name'

    POST_HISTORY_ID = 'post_history_id'

    POST_ID = 'post_id'

    INGESTION_TIMESTAMP_FIELD = 'ingestion_timestamp'

    POST_HISTORY_TS = 'timestamp'

    NUMERICAL_INT_FIELD = ['curr_engagement', 'num_of_likes', 'num_of_views', 'num_of_comments']

    # the name is after preprocess
    POST_HISTORY_VARIABLES = ['timestamp', 'curr_engagement', 'post_fk', 'post_history_id',
                              'num_of_likes', 'num_of_views', 'num_of_comments']
    POST_VARIABLES = ['url', 'publish_date', 'location', 'content', 'type', 'media_url', 'image_url', 'sentiment',
                      'post_id', 'creator_fk', 'platform_type']

    column_fields_from_scooper = [
        'parent_url', 'published_ts', 'source_country', 'source_country_code', 'source_region',
        'source_city', 'source_latitude', 'source_longitude', 'content', 'post_type', 'media_url',
        'image_url', 'sentiment', 'creator_id', 'engagement_score_view',
        'ingestion_timestamp', 'num_comments', 'facebook_likes', 'youtube_views',
        'twitter_shares', 'facebook_shares', 'youtube_likes', 'facebook_reactions_total', 'platform_type'
    ]

    map_from_field_name_from_scooper = {
        'parent_url': 'url',
        'publish_date': 'publish_date',
        'location': 'location',
        'content': 'content',
        'post_type': 'type',
        'media_url': 'media_url',
        'image_url': 'image_url',
        'sentiment': 'sentiment',
        'engagement_score_view': 'curr_engagement',
        'creator_id': 'creator_fk',
        'num_comments': 'num_of_comments'
    }

    VIEW_COUNT = 'num_of_views'
    LIKE_COUNT = 'num_of_likes'

    SUCCESS_RESPONSE = 200

    SUCCESS = "SUCCESS"

    NEW_CREATORS_INDICATOR = 'new_creators_indicator'

    location_fields = ['source_country', 'source_country_code', 'source_region', 'source_city', 'source_latitude',
                       'source_longitude']

    TIMESTAMP_PARTITION_ID = 'id'


class CreatorUtils:
    query_raw_data_fields = [
        'creator_id',
        'name',
        'sentiment',
        'creator_image',
        'creator_url',
        'language',
        'media_url',
        'ingestion_timestamp'
    ]

    CREATOR_FIELDS = [
        'creator_id',
        'name',
        'creator_image',
        'creator_url',
        'language',
        'is_verified',
        'hashtag_list',
        'engagement_history',
        'last_post_date',
        'platform_type',
    ]
    CREATOR_HISTORY_VARIABLES = ['owner_post_count', 'owner_follower_count', 'creator_fk', 'creator_history_id',
                                 'ingestion_timestamp']

    valid_platforms = ['twitter', 'youtube', 'facebook', 'instagram', 'tiktok', 'reddit', 'threads']

    primary_key = ['name', 'platform_type']

    NEW_CREATORS_ID_EXIST = 'new_creators_id_exist'

    CREATOR_HISTORY_ID = 'creator_history_id'

    CREATOR_ID = 'creator_id'

    CREATOR_ID_YEAR = 'creator_id_y'

    INGESTION_TIMESTAMP_FIELD = 'created_at'

    is_numeric = ['', '', '']

    TIMESTAMP_PARTITION_ID = 'id'
