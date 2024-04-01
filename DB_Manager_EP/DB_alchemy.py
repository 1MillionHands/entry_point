from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
import table_objects as tbls
import uuid
import json

with open('config_file.json', 'r') as f:
  config_data = json.load(f)

# * the name are after preprocess
POST_HISTORY_VARIABLES = ['num_of_likes', 'num_of_views', 'timestamp', 'scrape_status', 'curr_engagement',
                          'num_of_comments', 'video_play_count', 'video_view_count', 'virality_score', 'post_fk',
                          'post_history_id']
POST_VARIABLES = ['url', 'publish_date', 'location', 'content', 'type', 'media_url', 'image_url', 'sentiment',
                  'post_id', 'creator_fk', 'platform_type', 'virality_score']
CREATOR_HISTORY_VARIABLES = ['owner_post_count', 'owner_follower_count', 'creator_fk', 'creator_history_id']
CREATOR_VARIABLES = ['creator_image', 'name', 'creator_url', 'is_verified', 'creator_id']
# CREATOR_VARIABLES = ['creator_image', 'name', 'creator_url', 'is_verified']

def create_local_engine(test = False):
    HOST = config_data['aws_db']['host']
    PORT = config_data['aws_db']['port']
    DBNAME = config_data['aws_db']['dbname']
    SCHEMA = "omh_schema_test" if test else "omh_schema"
    USER = config_data['aws_db']['user']
    PASSWORD = config_data['aws_db']['password']

    # Define the database connection URL
    DATABASE_URL = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?options=-csearch_path%3D{SCHEMA}'
    # Create a SQLAlchemy engine
    engine = create_engine(DATABASE_URL)

    return engine

def get_db(engine):
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = SessionLocal()
    try:
      yield db
    finally:
      db.close()

class DbService:

    def __init__(self, engine):
        self.engine = engine


    def create_table(self, tbl_obj ):
        # Check if the table exists
        if not self.engine.dialect.has_table(self.engine.connect(), tbl_obj.__tablename__):
            # If the table does not exist, create it
            # metadata = MetaData()
            tbl_obj.metadata.create_all(self.engine)
            # tbl_obj.metadata = metadata  # Associate metadata if not already set
            # metadata.create_all(self.engine)
        else:
            print("Table name already exist in the db")

    def create_schema(self, db, schema_name ):
        # Check if the table exists
        try:
            db.execute(text("CREATE SCHEMA " + schema_name))
            db.commit()
        except Exception as f:
            print(f)

    @staticmethod
    def insert_table(db, tbl_obj, headers):
        db.bulk_insert_mappings(tbl_obj, headers)
        db.commit()

    def query_table(self, table_name, content, headers):
        pass

    def delete_table(self, table_name, content, headers):
        pass

    def update_table(self, table_name, content, headers):
        pass

    def run_query(self):
        # self.engine.
        pass


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
    # todo: refactor the fun; split it; check its functionalities
    # It split the posts to post id and post history
    #

    posts_list_not_exist_in_db = []
    posts_list_exist_in_db = []
    # Extract all URLs from the posts_data_list
    urls_to_check = [post_data['url'] for post_data in posts_data_list]

    # Query existing posts based on multiple URLs
    existing_posts = session.query(tbls.Post).filter(tbls.Post.url.in_(urls_to_check)).all()
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
    session.bulk_insert_mappings(tbls.Post, selected_cols_post_table)
    posts_list_not_exist_in_db = convert_names_in_list(posts_list_not_exist_in_db, 'post_id', 'post_fk')
    concatenated_list = posts_list_not_exist_in_db + posts_list_exist_in_db

    selected_cols_post_history_table = [{col: post[col] for col in POST_HISTORY_VARIABLES if col in post} for post in
                                        concatenated_list]

    session.bulk_insert_mappings(tbls.PostHistory, selected_cols_post_history_table)

    post_id_values_to_update = [d['post_fk'] for d in posts_list_not_exist_in_db]
    post_history_id_values_to_update = [d['post_history_id'] for d in posts_data_list]
    return post_id_values_to_update, post_history_id_values_to_update


def insert_creators_bulk_ep(session, posts_data_list):
    creators_list_not_exist_in_db = []
    creators_list_exist_in_db = []

    # Extract all URLs from the posts_data_list (should be part of Let the bot work
    names_to_check = [post_data['name'] for post_data in posts_data_list]

    # Query existing posts based on multiple URLs
    # todo: update creator list function (240-257)
    existing_creators = session.query(tbls.Creator).filter(tbls.Creator.name.in_(names_to_check)).all()
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
    session.bulk_insert_mappings(tbls.Creator, selected_cols_creators_table)
    creators_list_not_exist_in_db = convert_names_in_list(creators_list_not_exist_in_db, 'creator_id', 'creator_fk')
    concatenated_list = creators_list_not_exist_in_db + creators_list_exist_in_db
    selected_cols_exist_in_db_hist = [{col: post[col] for col in CREATOR_HISTORY_VARIABLES if col in post} for post in
                                      concatenated_list]
    # Insert the collection of objects into the database
    session.bulk_insert_mappings(tbls.CreatorHistory, selected_cols_exist_in_db_hist)
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
