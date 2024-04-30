from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, ForeignKey, Boolean, Float, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from enum import Enum as PythonEnum, auto
import uuid
Base = declarative_base()
from abc import ABC, abstractmethod


class DbObject():

    @abstractmethod
    def transform_data(self, **kwargs):
        pass



# Define the Posts model
class Post(Base, DbObject):
    __tablename__ = 'Post'

    post_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    # creator_fk = Column(String, ForeignKey('Creator.creator_id'))
    publish_date = Column(DateTime, default=datetime(2000, 1, 1))
    removed_date = Column(DateTime, default=datetime(2000, 1, 1))
    url = Column(String)
    location = Column(String, default='')
    description = Column(String, default='')
    content = Column(String, default='')
    type = Column(String, default='')
    sentiment = Column(String)  # -1 pro Gaza 0 Neutral 1 pro Israel
    media_url = Column(String, default='')
    image_url = Column(String, default='')
    is_live = Column(Boolean, default=True)
    subtitles_video = Column(String, default='')
    media_text = Column(String, default='')  # media_text(img/video)
    video_length = Column(Integer, default=0)  # in seconds
    media_size = Column(Integer, default=0)  # in MB (platform standartization)
    num_of_chars = Column(Integer, default=0)
    num_of_hashtags = Column(Integer, default=0)
    list_of_topics = Column(String, default='')
    language = Column(String, default='')
    date_archive = Column(DateTime, default=datetime(2000, 1, 1))
    virality_score = Column(Float, default=0)

    _table_args_ = {'schema': 'omh_schema'}

    def _init_(self):
        self.platform_mapping = {
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
        self.column_names_mapping = {
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

    def transform_data(self, posts):
        # todo: improve algorithm
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
                    post[key] = self.platform_mapping.get(post[key])
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
        return posts

    def match_headers(self, posts):
        # todo: improve algorithm
        for entry in posts:
            for old_name, new_name in self.column_names_mapping.items():
                if old_name in entry:
                    entry[new_name] = entry.pop(old_name)

        return posts

class PlatformType(PythonEnum):
    LINKEDIN = auto()
    INSTAGRAM = auto()
    YOUTUBE = auto()
    TWITTER = auto()
    FACEBOOK = auto()
    TIKTOK = auto()


class PostHistory(Base, DbObject):
    __tablename__ = 'PostHistory'

    post_history_id = Column('postHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    # post_fk = Column(String, ForeignKey('Post.post_id'))
    num_of_likes = Column(Integer, default=0)
    num_of_views = Column(Integer, default=0)
    timestamp = Column(DateTime, default=datetime(2000, 1, 1))  # last scrape time
    scrape_status = Column(String, default='unscraped')
    curr_engagement = Column(Integer, default=0)
    num_of_comments = Column(Integer, default=0)
    video_play_count = Column(Integer, default=0)
    video_view_count = Column(Integer, default=0)
    volunteer_engagement = Column(Integer, default=0)
    virality_score = Column(Float, default=0)


class Creatort(Base, DbObject):
    __tablename__ = 'Creator'

    creator_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String, default='no_name')
    volunteer_engagement = Column(Integer, default=0)
    num_of_deleted_posts = Column(Integer, default=0)
    sentiment = Column(Integer, default=0)
    creator_image = Column(String)
    creator_url = Column(String, default='')
    niche = Column(String, default='')
    geo_location = Column(String, default='')
    language = Column(String, default='')  # maybe list
    is_verified = Column(Boolean, default=False)
    hashtag_list = Column(String, default='')
    engagement_history = Column(String, default='')
    last_post_date = Column(DateTime, default=datetime(2000, 1, 1))


class CreatorHistoryt(Base, DbObject):
    __tablename__ = 'CreatorHistory'

    creator_history_id = Column('creatorHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    creator_fk = Column(String, ForeignKey('Creator.creator_id'))
    owner_post_count = Column(Integer, default=0)
    owner_follower_count = Column(Integer, default=0)
    creator_score = Column(Integer, default=0)


# class MetaDataBuckett(Base, DbObject):
#     _tablename_ = 'meta_data_bucket'
#
#     id = Column(String, primary_key=True, default=str(uuid.uuid4()))
#     source_fk = Column(String, ForeignKey('meta_data_sources.id'))
#     date = Column(DateTime)
#     content = Column(String)
#
#
# class MetaDataSourcest(Base, DbObject):
#     _tablename_ = 'meta_data_sources'
#
#     id = Column(String, primary_key=True, default=str(uuid.uuid4()))
#     name = Column(String)
#     url = Column(String)

class Volunteer(Base, DbObject):
    __tablename__ = 'meta_data_sources'

    volunteer_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    first_name = Column(String)
    last_name = Column(String)
    url = Column(String)