from datetime import datetime
from enum import Enum as PythonEnum, auto
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, TIMESTAMP, \
    Boolean, Float, Enum, event, func, Date, JSON, ARRAY, Numeric
from sqlalchemy.ext.declarative import declarative_base
import uuid
Base = declarative_base()


class ActiveFenceRowData(Base):
    __tablename__ = 'active_fence_ingestion_raw_data'
    timestamp = Column(TIMESTAMP, server_default=func.now())
    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    content_id = Column(String)
    content_type = Column(String)
    date_posted = Column(Date)
    description = Column(Text)
    discovery_input = Column(JSON)
    display_urls = Column(String)
    engagement_score_view = Column(Float)
    has_handshake = Column(Boolean)
    hashtags = Column(ARRAY(String))
    input = Column(JSON)
    latest_comments = Column(JSON)
    likes = Column(Numeric)
    location = Column(ARRAY(String)) # need transformation from list -> string
    num_comments = Column(Numeric)
    photos = Column(ARRAY(String))  # need transformation from list -> string
    pk = Column(String)
    post_id = Column(String)
    product_type = Column(String)
    shortcode = Column(String)
    tagged_users = Column(JSON)
    thumbnail = Column(String)
    url = Column(String)
    user_posted = Column(String)
    video_play_count = Column(Numeric)
    video_view_count = Column(Numeric)
    videos = Column(String)  # need transformation from list -> string
