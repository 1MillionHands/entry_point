from datetime import datetime
from enum import Enum as PythonEnum, auto
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, TIMESTAMP, \
    Boolean, Float, func
from sqlalchemy.ext.declarative import declarative_base
import uuid
Base = declarative_base()


class Post(Base):
    __tablename__ = 'Post'

    post_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    # creator_fk = Column(String, ForeignKey('Creator.creator_id')) todo: remove when program run in production
    creator_id = Column(String, ForeignKey('Creator.creator_id'))
    publish_date = Column(DateTime, default=datetime(2000, 1, 1))
    removed_date = Column(DateTime, default=datetime(2000, 1, 1))
    url = Column(String)
    location = Column(String, default='')
    description = Column(String, default='')
    content = Column(String, default='')
    type = Column(String, default='')
    sentiment_score = Column(String)  # -1 pro Gaza 0 Neutral 1 pro Israel
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


class PlatformType(PythonEnum):
    LINKEDIN = auto()
    INSTAGRAM = auto()
    YOUTUBE = auto()
    TWITTER = auto()
    FACEBOOK = auto()
    TIKTOK = auto()


class PostHistory(Base):
    __tablename__ = 'PostHistory'

    post_history_id = Column('postHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    post_id = Column(String, ForeignKey('Post.post_id'))
    num_of_likes = Column(Integer, default=0)
    num_of_views = Column(Integer, default=0)
    ingestion_timestamp = Column(DateTime, default=datetime(2000, 1, 1))  # last scrape time
    scrape_status = Column(String, default='unscraped')
    engagement_score_view = Column(Float, default=0)
    num_of_comments = Column(Integer, default=0)
    video_play_count = Column(Integer, default=0)
    video_view_count = Column(Integer, default=0)
    virality_score = Column(Float, default=0)


class Creatort(Base):
    __tablename__ = 'Creator'

    creator_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    creator_name = Column(String, default='no_name')
    num_of_deleted_posts = Column(Integer, default=0)
    sentiment_score = Column(Integer, default=0)
    creator_image = Column(String)
    creator_url = Column(String, default='')
    niche = Column(String, default='')
    geo_location = Column(String, default='')
    language = Column(String, default='')  # maybe list
    is_verified = Column(Boolean, default=False)
    hashtag_list = Column(String, default='')
    engagement_history = Column(String, default='')
    last_post_date = Column(DateTime, default=datetime(2000, 1, 1))
    platform_name = Column(String, default=str(uuid.uuid4()))

class ScoperTemp(Base):
    __tablename__ = 'temp_scooper'

    creator_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    creator_name = Column(String, default='no_name')
    sentiment_score = Column(Integer, default=0)
    creator_image = Column(String)
    creator_url = Column(String, default='')
    language = Column(String, default='')  # maybe list
    platform_name = Column(String, default=str(uuid.uuid4()))

class CreatorHistoryt(Base):
    __tablename__ = 'CreatorHistory'

    creator_history_id = Column('creatorHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    creator_id = Column(String, ForeignKey('Creator.creator_id'))
    owner_post_count = Column(Integer, default=0)
    owner_follower_count = Column(Integer, default=0)
    creator_score = Column(Integer, default=0)
    ingestion_timestamp = Column(TIMESTAMP, server_default=func.now())


# class MetaDataBuckett(Base):
#     __tablename__ = 'meta_data_bucket'
#
#     id = Column(String, primary_key=True, default=str(uuid.uuid4()))
#     source_fk = Column(String, ForeignKey('meta_data_sources.id'))
#     date = Column(DateTime)
#     content = Column(String)
#
#
# class MetaDataSourcest(Base):
#     __tablename__ = 'meta_data_sources'
#
#     id = Column(String, primary_key=True, default=str(uuid.uuid4()))
#     name = Column(String)
#     url = Column(String)

class Volunteer(Base):
    __tablename__ = 'meta_data_sources'

    volunteer_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    first_name = Column(String)
    last_name = Column(String)
    url = Column(String)

# class BaseMixin(object):
#
#   __table_args__ = {'mysql_engine': 'InnoDB'}
#
#   id = Column(Integer, primary_key=True)
#   ingestion_timestamp = Column('ingestion_timestamp', DateTime, nullable=False)
#   updated_at = Column('updated_at', DateTime, nullable=False)
#
#   @staticmethod
#   def create_time(instance):
#      now = datetime.datetime.utcnow()
#      instance.ingestion_timestamp = now
#      instance.updated_at = now
#
#   @staticmethod
#   def update_time(instance):
#      now = datetime.datetime.utcnow()
#      instance.updated_at = now
#
#   @classmethod
#   def register(cls):
#      event.listen(cls, 'before_insert', cls.create_time)
#      event.listen(cls, 'before_update', cls.update_time)