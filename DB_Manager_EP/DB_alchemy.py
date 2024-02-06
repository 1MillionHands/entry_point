from sqlalchemy import create_engine, Column, Integer, String, DateTime, text, ForeignKey, Boolean, Float, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import uuid
from Data_Service import get_config
from enum import Enum as PythonEnum, auto

# * the name are after preprocess
POST_HISTORY_VARIABLES = ['num_of_likes', 'num_of_views', 'timestamp', 'scrape_status', 'curr_engagement',
                          'num_of_comments', 'video_play_count', 'video_view_count', 'virality_score', 'post_fk',
                          'post_history_id']
POST_VARIABLES = ['url', 'publish_date', 'location', 'content', 'type', 'media_url', 'image_url', 'sentiment',
                  'post_id', 'creator_fk', 'platform_type', 'virality_score']
CREATOR_HISTORY_VARIABLES = ['owner_post_count', 'owner_follower_count', 'creator_fk', 'creator_history_id']
CREATOR_VARIABLES = ['creator_image', 'name', 'creator_url', 'is_verified', 'creator_id']
# CREATOR_VARIABLES = ['creator_image', 'name', 'creator_url', 'is_verified']


HOST = get_config()['aws_db']['host']
PORT = get_config()['aws_db']['port']
DBNAME = get_config()['aws_db']['dbname']
USER = get_config()['aws_db']['user']
PASSWORD = get_config()['aws_db']['password']
# Define the database connection URL
DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}'
# Create a SQLAlchemy engine
engine = create_engine(DATABASE_URL, echo=True)
# Create a session factory
Session = sessionmaker(bind=engine)
# Create a base class for declarative models
Base = declarative_base()


class PlatformType(PythonEnum):
    LINKEDIN = auto()
    INSTGRAM = auto()
    YOUTUBE = auto()
    TWITTER = auto()
    FACEBOOK = auto()
    TIKTOK = auto()


# Define the Posts model
class Post(Base):
    __tablename__ = 'Post'

    post_id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    creator_fk = Column(String, ForeignKey('Creator.creator_id'))
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
    platform_type = Column(Enum(PlatformType))


class PostHistory(Base):
    __tablename__ = 'PostHistory'

    post_history_id = Column('postHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    post_fk = Column(String, ForeignKey('Post.post_id'))
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


class Creator(Base):
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


class CreatorHistory(Base):
    __tablename__ = 'CreatorHistory'

    creator_history_id = Column('creatorHistory_id', String, primary_key=True, default=str(uuid.uuid4()))
    creator_fk = Column(String, ForeignKey('Creator.creator_id'))
    owner_post_count = Column(Integer, default=0)
    owner_follower_count = Column(Integer, default=0)
    creator_score = Column(Integer, default=0)


class MetaDataBucket(Base):
    __tablename__ = 'meta_data_bucket'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    source_fk = Column(String, ForeignKey('meta_data_sources.id'))
    date = Column(DateTime)
    content = Column(String)


class MetaDataSources(Base):
    __tablename__ = 'meta_data_sources'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String)
    url = Column(String)


def convert_names_in_list(list_, old_column_name, new_column_name):
    # Iterate through the list of dictionaries and update the key
    try:
        for item in list_:
            if old_column_name in item:
                item[new_column_name] = item.pop(old_column_name)
        return list_
    except:
        print("old column name might not be in the list")
        return list_


# def update_posts_with_creator_id(post_list, session):
#     names_to_check = [post_data['name'] for post_data in post_list]
#     # Query the database to get creator_id based on the 'name' column
#     existing_creators = session.query(Creator).filter(Creator.name.in_(names_to_check)).all()
#     # creators = session.query(Creator.name, Creator.creator_id).all()
#     for post_data in post_list:
#         try:
#             existing_post = next(post for post in existing_creators if post.name == post_data['name'])
#             # Handle the case where a matching post was found
#             post_data['creator_fk'] = existing_post.creator_id
#         except StopIteration:
#             # Handle the case where no matching post was found
#             print("No existing post found")
#             break
#     return post_list


# def append_posts_fk_into_existis_posts(session, post_data_list):
#     urls_to_check = [post_data['url'] for post_data in posts_data_list]
#     # Query existing posts based on multiple URLs
#     existing_posts = session.query(Posts).filter(Posts.url.in_(urls_to_check)).all()
#
#     for post_data in posts_data_list:
#         try:
#             # Get the existing post from the database
#             existing_post = next(post for post in existing_posts if post.url == post_data['url'])
#             # Add the post_fk field to the existing post
#             post_data['post_fk'] = existing_post.id
#         except StopIteration:
#             break
#
#     return post_data_list


def insert_posts_bulk_ep(session, posts_data_list):
    posts_list_not_exist_in_db = []
    posts_list_exist_in_db = []
    # Extract all URLs from the posts_data_list
    urls_to_check = [post_data['url'] for post_data in posts_data_list]

    # Query existing posts based on multiple URLs
    existing_posts = session.query(Post).filter(Post.url.in_(urls_to_check)).all()
    existing_post_urls = {post.url for post in existing_posts}

    for post_data in posts_data_list:
        post_data['post_history_id'] = str(uuid.uuid4())
        if post_data['url'] not in existing_post_urls:
            post_data['post_id'] = str(uuid.uuid4())
            posts_list_not_exist_in_db.append(post_data)
        else:
            try:
                # Get the existing post from the database
                existing_post = next(post for post in existing_posts if post.url == post_data['url'])
                # Add the post_fk field to the existing post
                post_data['post_fk'] = existing_post.post_id
                posts_list_exist_in_db.append(post_data)
            except StopIteration:
                break

    selected_cols_post_table = [{col: post[col] for col in POST_VARIABLES if col in post} for post in
                                posts_list_not_exist_in_db]
    # Insert the collection of objects into the database
    session.bulk_insert_mappings(Post, selected_cols_post_table)
    posts_list_not_exist_in_db = convert_names_in_list(posts_list_not_exist_in_db, 'post_id', 'post_fk')
    concatenated_list = posts_list_not_exist_in_db + posts_list_exist_in_db

    selected_cols_post_history_table = [{col: post[col] for col in POST_HISTORY_VARIABLES if col in post} for post in
                                        concatenated_list]

    session.bulk_insert_mappings(PostHistory, selected_cols_post_history_table)

    post_id_values_to_update = [d['post_fk'] for d in posts_list_not_exist_in_db]
    post_history_id_values_to_update = [d['post_history_id'] for d in posts_data_list]
    return post_id_values_to_update, post_history_id_values_to_update


def insert_creators_bulk_ep(session, posts_data_list):
    creators_list_not_exist_in_db = []
    creators_list_exist_in_db = []
    # Extract all URLs from the posts_data_list
    names_to_check = [post_data['name'] for post_data in posts_data_list]

    # Query existing posts based on multiple URLs
    existing_creators = session.query(Creator).filter(Creator.name.in_(names_to_check)).all()
    existing_names = {creator.name for creator in existing_creators}

    for post_data in posts_data_list:
        post_data['creator_history_id'] = str(uuid.uuid4())
        if post_data['name'] not in existing_names:
            post_data['creator_id'] = str(uuid.uuid4())
            creators_list_not_exist_in_db.append(post_data)
        else:
            try:
                # Get the existing post from the database
                existing_creator = next(creator for creator in existing_creators if creator.name == post_data['name'])
                # Add the post_fk field to the existing post
                post_data['creator_fk'] = existing_creator.creator_id
                creators_list_exist_in_db.append(post_data)
            except StopIteration:
                print(f"Warning: No existing creator found for name {post_data['name']}")

    # Extract only specified columns from each dictionary in posts_data_list
    selected_cols_creators_table = [{col: post[col] for col in CREATOR_VARIABLES if col in post} for post in
                                    creators_list_not_exist_in_db]
    session.bulk_insert_mappings(Creator, selected_cols_creators_table)
    creators_list_not_exist_in_db = convert_names_in_list(creators_list_not_exist_in_db, 'creator_id', 'creator_fk')
    concatenated_list = creators_list_not_exist_in_db + creators_list_exist_in_db
    selected_cols_exist_in_db_hist = [{col: post[col] for col in CREATOR_HISTORY_VARIABLES if col in post} for post in
                                      concatenated_list]
    # Insert the collection of objects into the database
    session.bulk_insert_mappings(CreatorHistory, selected_cols_exist_in_db_hist)
    return posts_data_list





def insert_posts_and_creators_ep(posts_data_list):
    # Create a session
    session = Session()
    posts_data_list = insert_creators_bulk_ep(session, posts_data_list)
    post_id_values_to_update, post_history_id_values_to_update = insert_posts_bulk_ep(session, posts_data_list)
    # Commit the transaction
    session.commit()
    # Close the session
    session.close()
    return post_id_values_to_update, post_history_id_values_to_update

# Create the table in the database
# Base.metadata.create_all(engine)
