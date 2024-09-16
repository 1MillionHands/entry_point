from datetime import datetime
from enum import Enum as PythonEnum, auto
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, TIMESTAMP, \
    Boolean, Float, func, Enum
from sqlalchemy.ext.declarative import declarative_base
import uuid
Base = declarative_base()



class PlatformType(PythonEnum):
    LINKEDIN = auto()
    INSTAGRAM = auto()
    YOUTUBE = auto()
    TWITTER = auto()
    FACEBOOK = auto()
    TIKTOK = auto()
    REDDIT = auto()


class Post(Base):
    __tablename__ = 'Post'

    post_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    creator_fk = Column(String, ForeignKey('Creator.creator_id')) #todo: remove when program run in production
    # creator_id = Column(String, ForeignKey('Creator.creator_id'))
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
    platform_type = Column(String)


class PostHistory(Base):
    __tablename__ = 'PostHistory'

    post_history_id = Column('postHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    post_fk = Column(String, ForeignKey('Post.post_id'))
    num_of_likes = Column(Integer, default=0)
    num_of_views = Column(Integer, default=0)
    timestamp = Column(DateTime, default=datetime(2000, 1, 1))  # last scrape time
    scrape_status = Column(String, default='unscraped')
    curr_engagement = Column(Float, default=0)
    num_of_comments = Column(Integer, default=0)
    video_play_count = Column(Integer, default=0)
    video_view_count = Column(Integer, default=0)
    virality_score = Column(Float, default=0)


class Creatort(Base):
    __tablename__ = 'Creator'

    creator_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String, default='no_name')
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
    platform_type = Column(String)


class CreatorHistoryt(Base):
    __tablename__ = 'CreatorHistory'

    creator_history_id = Column('creatorHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    creator_key = Column(String, ForeignKey('Creator.creator_id'))
    owner_post_count = Column(Integer, default=0)
    owner_follower_count = Column(Integer, default=0)
    creator_score = Column(Integer, default=0)
    created_at = Column(TIMESTAMP, server_default=func.now())


class Volunteer(Base):
    __tablename__ = 'meta_data_sources'

    volunteer_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    first_name = Column(String)
    last_name = Column(String)
    url = Column(String)
