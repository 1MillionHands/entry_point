import uuid

# Third-party libraries
import pandas as pd

# Local application/library specific imports
from DB_Manager_EP.db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory
from utils import *
from table_objects.table_handler import TableHandler
from utils import PostUtils



class PostHandler(TableHandler):

    def __init__(self, data, event):
        super().__init__(data, event)

    def run(self, run_type):

        if run_type == 'run_from_api':
            self.run_from_api()
        else:
            self.run_from_circle()

    def run_from_api(self):
        """
        Orchestrates the workflow to upload, process, update, and communicate changes in data.
        :return: None
        """

        self.upload_to_s3(self.event['s3_filename'])  # todo: check if you can use a ready function in data service
        self.match_db_field_names()
        self.preprocess_posts_to_fit_db()
        post_id_values_to_update, post_history_id_values_to_update = self.update_db()
        # close the circle - todo: implement the circle
        # msg = self.create_messages(post_id_values_to_update, post_history_id_values_to_update)
        # self.send_messages(msg)

    def run_from_circle(self):
        pass


    def update_db(self):
        """
        Updates the database by inserting new creators and posts, returning IDs for updates.
        :return: tuple (list, list) of post ID values to update and post history ID values to update
        """

        self.insert_creators_bulk()
        post_id_values_to_update, post_history_id_values_to_update = self.insert_posts_bulk()
        return post_id_values_to_update, post_history_id_values_to_update


    def insert_creators_bulk(self):
        """
        Performs bulk insertion of new creators and updates existing ones in the database.
        :return: None
        """
        # filters = [
        #     {
        #         "column": "name",
        #         "values": list(self.data_df.name)
        #     },
        #     {
        #         "column": "platform_type",
        #         "values": list(self.data_df.platform_type)
        #     }
        # ]
        # existing_creators = self.db_obj.query_table_orm(table_name=Creatort, columns=[Creatort.name,Creatort.platform_type], filters=filters, distinct =True, to_df=True)
        existing_creators = self.db_obj.filter_query_table(Creatort, [Creatort.name, Creatort.platform_type], [list(self.data_df.name), list(self.data_df.platform_type)], True, True)
        creator_exist_in_db = existing_creators[['name', PostUtils.CREATOR_ID,'platform_type']].rename(columns={PostUtils.CREATOR_ID: 'creator_id_temp'})
        self.data_df = self.data_df.merge(creator_exist_in_db, how='left', on=['name','platform_type'])
        self.data_df[PostUtils.NEW_CREATORS_INDICATOR] = self.data_df[['creator_id_temp']].isnull()
        self.data_df[PostUtils.CREATOR_ID] = [str(uuid.uuid4()) if empty_id_cond else id for id, empty_id_cond in self.data_df[['creator_id_temp', PostUtils.NEW_CREATORS_INDICATOR]].values]
        self.data_df[PostUtils.CREATOR_HISTORY_ID] = [str(uuid.uuid4()) for i in range(self.data_df.shape[0])]

        current_cols = self.columns_exist_in_external_data(PostUtils.CREATOR_FIELDS, self.data_df.columns)
        creators = self.data_df[self.data_df[PostUtils.NEW_CREATORS_INDICATOR]][current_cols].to_dict(orient='records')
        self.db_obj.insert_table(Creatort, creators)

        current_cols = self.columns_exist_in_external_data(PostUtils.CREATOR_HISTORY_VARIABLES, self.data_df.columns)
        self.db_obj.insert_table(CreatorHistoryt, self.data_df[current_cols].to_dict(orient='records'))


    def insert_posts_bulk(self):
        """
        Performs bulk insertion of new posts and updates existing ones in the database.
        :return: tuple (list, list) of updated post ID values and post history ID values
        """

        def find_out_of_range_values(df, min_value=-2147483648, max_value=2147483647):
            out_of_range_rows = []
            for idx, row in df.iterrows():
                if any(isinstance(value, int) and (value < min_value or value > max_value) for value in row):
                    print(f"Row {idx} has out-of-range value.")
                    out_of_range_rows.append(idx)
            return out_of_range_rows
        existing_posts = self.db_obj.query_table_orm(Post, Post.url, list(self.data_df.url),True, True)
        # filters = [
        #     {
        #         "column": "name",
        #         "values": list(self.data_df.name)
        #     },
        #     {
        #         "column": "platform_type",
        #         "values": list(self.data_df.platform_type)
        #     }
        # ]
        # existing_creators = self.db_obj.query_table_orm(table_name=Post,columns=[Creatort.name, Creatort.platform_type], filters=filters, distinct=True, to_df=True)
        if existing_posts[0] is not None:
            self.data_df = self.data_df.merge(existing_posts, how='left', on='url')

            self.data_df['new_posts_indicator'] = self.data_df[PostUtils.POST_ID].isnull()
            self.data_df[PostUtils.POST_ID] = self.data_df[PostUtils.POST_ID].apply(lambda x: str(uuid.uuid4()) if x is None else x)

            new_posts = self.data_df[self.data_df['new_posts_indicator']]
        else:
            self.data_df[PostUtils.POST_ID] = [str(uuid.uuid4()) for x in range(self.data_df.shape[0])]
            new_posts = self.data_df.copy()

        current_cols = self.columns_exist_in_external_data(PostUtils.POST_VARIABLES, self.data_df.columns)
        self.db_obj.insert_table(Post, new_posts[current_cols].to_dict(orient='records'))

        self.data_df[PostUtils.POST_HISTORY_ID] = str(uuid.uuid4())

        if len(find_out_of_range_values(self.data_df[current_cols])) >= 1:
            raise "One of the post data contain to high value"

        posts_hst = self.data_df[current_cols].to_dict(orient='records')
        self.db_obj.insert_table(PostHistory, posts_hst)

        post_id_values_to_update = list(new_posts[PostUtils.POST_ID])
        post_history_id_values_to_update = list(self.data_df[PostUtils.POST_HISTORY_ID])
        return post_id_values_to_update, post_history_id_values_to_update


    @staticmethod
    def get_message(history_ids, posts_ids):
        """
        Constructs a message containing the post and history IDs.
        :param history_ids: List of history IDs to be included in the message
        :param posts_ids: List of post IDs to be included in the message
        :return: dict containing post and history IDs
        """
        return {'POST_IDS': posts_ids, 'HISTORY_IDS': history_ids}


    def create_messages(self, post_ids_to_update, history_ids_to_update):
        """
        Creates messages in batches from updated post and history IDs for further processing or notification.
        :param post_ids_to_update: List of post IDs that have been updated
        :param history_ids_to_update: List of history IDs that have been updated
        :return: List of message dictionaries
        """

        messages = []
        history_size = len(history_ids_to_update)
        for i in range(0, history_size, POSTS_NUM):
            from_i = i
            to_i = i + POSTS_NUM
            post_size = len(post_ids_to_update)
            if to_i > post_size >= from_i:
                to_i = min(i + POSTS_NUM, post_size)
            elif to_i > post_size:
                to_i = min(i + POSTS_NUM, history_size)
                from_i = to_i
            post_id_slice = post_ids_to_update[from_i:to_i]
            post_history_id_slice = history_ids_to_update[from_i:to_i]
            message = self.get_message(post_history_id_slice, post_id_slice)
            messages.append(message)
        return messages


    def match_db_field_names(self):
        """
        Maps data fields from source format to database field names for compatibility.
        :return: Dictionary with data field names mapped to those expected by the database
        """

        column_names_mapping = {
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

        for entry in self.data:
            # Create a new dictionary for each entry with updated keys.
            # This will contain only the keys that need to be updated.
            updated_keys = {new_name: entry.pop(old_name) for old_name, new_name in column_names_mapping.items() if
                            old_name in entry}

            # Update the original entry dictionary with the new keys.
            entry.update(updated_keys)

        # for entry in self.data:
        #     for old_name, new_name in column_names_mapping.items():
        #         if old_name in entry:
        #             entry[new_name] = entry.pop(old_name)


        self.data_df.rename(columns=column_names_mapping, inplace=True)
        print('')


    def preprocess_posts_to_fit_db(self):
        """
        Preprocesses posts data to ensure compatibility with database constraints and rules.
        :param posts: List of dictionaries representing posts data
        :return: List of dictionaries with preprocessed post data
        """

        max_length = 255
        for post in self.data:
            for key, value in post.items():
                if post[key] == 'None' or str(post[key]).lower() == 'nan' or post[key] == "":
                    post[key] = None
                if (key == 'media_url' or key == 'creator_image') and isinstance(value, str) and len(
                        value) > max_length:
                    post[key] = value[:max_length]
                elif key == 'platform_type':
                    post[key] = PostUtils.platform_mapping.get(post[key])
                if key == 'curr_engagement' and value:
                    try:
                        post[key] = int(float(value))
                    except:
                        post[key] = 0
            if post['sentiment'] == False:
                post['sentiment'] = '-1'
            else:
                post['sentiment'] = '1'

        self.data_df = pd.DataFrame(self.data)


# if __name__ == '__main__':
#     let_bot = LetBotWork(config_data)
#     ep = EntryPoint(let_bot)
#     ep.get_posts()
#     while True:
#         print(f"initating scheduler every {config_data['schedule']['hours']} hours")
#         run_pending()
#         sleep(5)
