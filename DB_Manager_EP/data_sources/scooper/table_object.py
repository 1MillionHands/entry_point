from datetime import datetime
from enum import Enum as PythonEnum, auto
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, TIMESTAMP, \
    Boolean, Float, Enum, event, func, Date, JSON, ARRAY, Numeric
from sqlalchemy.ext.declarative import declarative_base
import uuid
Base = declarative_base()


class ScooperRowData(Base):
    __tablename__ = 'scooper_ingestion_raw_data'
    ingestion_timestamp = Column(TIMESTAMP, server_default=func.now())
    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    post_url = Column(String)
    image_url = Column(String)
    engagement_score_view = Column(Numeric)
    source_country = Column(String)
    reach = Column(Numeric)
    source_latitude = Column(String)
    source_country_code = Column(String)
    facebook_likes = Column(Numeric)
    media_url = Column(String)
    post_type = Column(String)
    twitter_shares = Column(Numeric)
    not_safe_for_work_level = Column(Numeric)
    content_snippet = Column(Text)
    indexed_datetime = Column(DateTime, nullable=True)
    entity_urls = Column(String)
    source_types_list = Column(String)
    published_datetime = Column(DateTime, nullable=True)
    source_longitude = Column(String)
    title_snippet = Column(Text)
    parent_url = Column(String)
    scooper_tags = Column(String)
    sentiment_score = Column(Numeric)
    author_gender = Column(String)
    host_url = Column(String)
    facebook_shares = Column(Numeric)
    semrush_pageviews = Column(Numeric)
    search_indexed_datetime = Column(String, nullable=True)
    published_ts = Column(TIMESTAMP)
    source_name = Column(String)
    search_indexed_ts = Column(TIMESTAMP)
    title = Column(Text)
    matched_profile = Column(String)
    content = Column(Text)
    alexa_pageviews = Column(Numeric)
    cluster_id = Column(String)
    word_count = Column(Numeric)
    num_comments = Column(Numeric)
    indexed_ts = Column(TIMESTAMP)
    language = Column(String)
    author_name = Column(String)
    facebook_reactions_total = Column(Numeric)
    source_continent = Column(String)
    url = Column(String)
    semrush_unique_visitors = Column(Numeric)
    author_id = Column(String)
    alexa_unique_visitors = Column(Numeric)
    source_region = Column(String)
    doc_score_avee = Column(Numeric)
    source_city = Column(String)
    porn_level = Column(Numeric)
    author_url = Column(String)
    author_image_url = Column(String)
    youtube_views = Column(Numeric)
    youtube_likes = Column(Numeric)
    author_short_name = Column(String)
    article_continent = Column(String)
    article_country = Column(String)
    article_latitude = Column(String)
    article_city = Column(String)
    article_region = Column(String)
    video_url = Column(String)
    article_longitude = Column(String)
    article_country_code = Column(String)
    provider = Column(String)

class Utils:

    INGESTION_COLUMN_NAMES = {
        'root_url': 'post_url',
        'images.url': 'image_url',
        'engagement': 'engagement_score_view',
        'extra_source_attributes.world_data.country': 'source_country',
        'reach': 'reach',
        'extra_source_attributes.world_data.latitude': 'source_latitude',
        'extra_source_attributes.world_data.country_code': 'source_country_code',
        'article_extended_attributes.facebook_likes': 'facebook_likes',
        'domain_url': 'media_url',
        'post_type': 'post_type',
        'article_extended_attributes.twitter_shares': 'twitter_shares',
        'nsfw_level': 'not_safe_for_work_level',
        'content_snippet': 'content_snippet',
        'indexed': 'indexed_datetime',
        'entity_urls': 'entity_urls',
        'source_type': 'source_types_list',
        'published': 'published_datetime',
        'extra_source_attributes.world_data.longitude': 'source_longitude',
        'title_snippet': 'title_snippet',
        'parent_url': 'parent_url',
        'tags_internal': 'scooper_tags',
        'sentiment': 'sentiment_score',
        'extra_author_attributes.gender': 'author_gender',
        'host_url': 'host_url',
        'article_extended_attributes.facebook_shares': 'facebook_shares',
        'source_extended_attributes.semrush_pageviews': 'semrush_pageviews',
        'search_indexed': 'search_indexed_datetime',
        'published_ts': 'published_ts',
        'extra_source_attributes.name': 'source_name',
        'search_indexed_ts': 'search_indexed_ts',
        'title': 'title',
        'matched_profile': 'matched_profile',
        'content': 'content',
        'source_extended_attributes.alexa_pageviews': 'alexa_pageviews',
        'cluster_id': 'cluster_id',
        'word_count': 'word_count',
        'article_extended_attributes.num_comments': 'num_comments',
        'indexed_ts': 'indexed_ts',
        'lang': 'language',
        'extra_author_attributes.name': 'author_name',
        'article_extended_attributes.facebook_reactions_total': 'facebook_reactions_total',
        'extra_source_attributes.world_data.continent': 'source_continent',
        'url': 'url',
        'source_extended_attributes.semrush_unique_visitors': 'semrush_unique_visitors',
        'extra_author_attributes.id': 'author_id',
        'source_extended_attributes.alexa_unique_visitors': 'alexa_unique_visitors',
        'extra_source_attributes.world_data.region': 'source_region',
        'docscore.avee': 'doc_score_avee',
        'extra_source_attributes.world_data.city': 'source_city',
        'porn_level': 'porn_level',
        'extra_author_attributes.url': 'author_url',
        'extra_author_attributes.image_url': 'author_image_url',
        'article_extended_attributes.youtube_views': 'youtube_views',
        'article_extended_attributes.youtube_likes': 'youtube_likes',
        'extra_author_attributes.short_name': 'author_short_name',
        'extra_article_attributes.world_data.continent': 'article_continent',
        'extra_article_attributes.world_data.country': 'article_country',
        'extra_article_attributes.world_data.latitude': 'article_latitude',
        'extra_article_attributes.world_data.city': 'article_city',
        'extra_article_attributes.world_data.region': 'article_region',
        'videos.url': 'video_url',
        'extra_article_attributes.world_data.longitude': 'article_longitude',
        'extra_article_attributes.world_data.country_code': 'article_country_code',
        'provider': 'provider'
    }

    NUMERIC_FIELDS = [
                        'engagement_score_view',
                        'reach',
                        'facebook_likes',
                        'twitter_shares',
                        'not_safe_for_work_level',
                        'sentiment_score',
                        'facebook_shares',
                        'semrush_pageviews',
                        'alexa_pageviews',
                        'cluster_id',
                        'word_count',
                        'num_comments',
                        'facebook_reactions_total',
                        'semrush_unique_visitors',
                        'alexa_unique_visitors',
                        'doc_score_avee',
                        'source_city',
                        'porn_level',
                        'youtube_views',
                        'youtube_likes'
        ]
