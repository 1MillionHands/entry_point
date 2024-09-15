import uuid

# Third-party libraries
import pandas as pd
import json

# Local application/library specific imports
from data_sources.scooper.table_object import ScooperRowData
from DB_Manager_EP.db_table_objects import Post, Creatort, PostHistory
from DB_Manager_EP.table_handler import TableHandler
from table_objects.utils import PostUtils, CreatorUtils


class PostHandler(TableHandler):

    def __init__(self, event):
        super().__init__(event)
        self.timestamp_partition_id = event[PostUtils.TIMESTAMP_PARTITION_ID]

    def run(self, run_type=None):

        if run_type == 'run_from_scooper':
            self.run_from_scooper()
        else:
            self.run_from_circle()

    def run_from_scooper(self):
        """
        Orchestrates the workflow to upload, process, update, and communicate changes in data.
        :return: None
        """

        self.df_data = self.query_raw_data()
        if self.df_data is None:
            # Early exit if no data is found
            print("No data found, stopping the process.")
            return

        new_posts_, posts_hst = self.transform()

        if len(new_posts_) > 0:
            self.update_db_insert(Post, new_posts_)

        if posts_hst is not None:
            self.update_db_delete_insert(PostHistory, posts_hst, PostUtils.INGESTION_TIMESTAMP_FIELD,
                                         [self.timestamp_partition_id])


    def run_from_circle(self):
        pass

    def query_raw_data(self):
        filters = [
            {
                "column": PostUtils.INGESTION_TIMESTAMP_FIELD,
                "values": [self.timestamp_partition_id],
                "op": "in"
            }
        ]
        tbl_result = self.db_obj.query_table_orm(ScooperRowData, filters=filters, to_df=True)
        if tbl_result[0] is None:
            return None
        else:
            return tbl_result[0]

    def update_db_insert(self, tbl_object=None, records=None):
        """
        Updates the database by inserting new creators and posts, returning IDs for updates.
        :return: tuple (list, list) of post ID values to update and post history ID values to update
        """
        self.db_obj.insert_table(tbl_object, records)

    def create_location(self):

        # Function to remove '_score' from the key and create a JSON
        def create_json(row, columns):
            # Create a dictionary where '_score' is removed from the column names, keeping the values intact
            return json.dumps({col.replace('source_', ''): row[col] for col in columns})

        #  concatenate by '\' all the fields located in self.df_data and PostUtils.location_fields
        self.df_data['location'] = self.df_data[PostUtils.location_fields].apply(
            lambda x: create_json(x, PostUtils.location_fields), axis=1)

    def transform(self):
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
        new_posts_ = pd.DataFrame()

        # drop rows with nan values from url parent
        self.df_data.dropna(subset=['parent_url'], inplace=True)
        # Adjust creator_id
        self.validate_creator_id()

        self.create_location()

        # convert published_ts to date
        self.df_data['publish_date'] = pd.to_datetime(self.df_data['published_ts'], unit='ms').dt.date
        self.df_data.rename(columns=PostUtils.map_from_field_name_from_scooper, inplace=True)

        filters = [
            {
                "column": "url",
                "values": self.df_data.url.values.tolist(),
                "op": "in"
            }
        ]
        existing_posts = self.db_obj.query_table_orm(Post, filters=filters, distinct=True, to_df=True)
        existing_posts = existing_posts[0]

        if existing_posts is not None:

            # filter from self.df_data all rows that exists in the existing posts
            self.df_data = self.df_data[~self.df_data.url.isin(existing_posts.url)]

            # create new post_id columns and generate at random
            self.df_data['post_id'] = [str(uuid.uuid4()) for x in range(self.df_data.shape[0])]

            new_posts = self.df_data.copy()

            # concatenate existing post from db to the df_data
            self.df_data = pd.concat([self.df_data, existing_posts], ignore_index=True)


        else:
            self.df_data[PostUtils.POST_ID] = [str(uuid.uuid4()) for x in range(self.df_data.shape[0])]
            new_posts = self.df_data.copy()

        if new_posts.shape[0] > 0:
            current_cols = self.columns_exist_in_external_data(PostUtils.POST_VARIABLES, self.df_data.columns)
            new_posts_ = new_posts[current_cols].to_dict(orient='records')

        self.df_data[PostUtils.POST_HISTORY_ID] = str(uuid.uuid4())

        # if len(find_out_of_range_values(self.df_data[current_cols])) >= 1:
        #     raise "One of the post data contain to high value"

        self.df_data['ingestion_timestamp'] = self.timestamp_partition_id
        posts_hst = self.df_data[PostUtils.POST_HISTORY_VARIABLES].to_dict(orient='records')
        return new_posts_, posts_hst

    def validate_creator_id(self):

        self.df_data['platform_name'] = self.df_data.media_url.apply(lambda x: self.extract_platform_name(x))
        filters = [
            {
                "column": "creator_name",
                "values": list(self.df_data.creator_name)
            },
            {
                "column": "platform_name",
                "values": list(self.df_data.platform_name)
            }
        ]

        existing_creators = self.db_obj.query_table_orm(table_name=Creatort, filters=filters, distinct=True, to_df=True)[0][['creator_id', 'creator_name', 'platform_name']]

        temp = self.df_data[['parent_url','creator_id', 'creator_name', 'platform_name']]
        merged_data = pd.merge(temp, existing_creators, on=['creator_name', 'platform_name'], suffixes=('_new', '_gt'), how='left')

        # Step 2: Update the 'id' in new_data with the 'id' from gt where there is a match
        # 'id_new' is from new_data and 'id_gt' is from gt
        merged_data['creator_id_new'] = merged_data['creator_id_gt']

        # Step 3: Drop the extra columns and keep the updated 'id_new' as 'id'
        updated_new_data = merged_data[['creator_name', 'platform_name', 'creator_id_new']].rename(columns={'creator_id_new': 'creator_id'})

        self.df_data['creator_id'] = updated_new_data['creator_id']

        self.df_data.dropna(subset=['creator_id'], inplace=True)

        self.df_data = self.df_data[PostUtils.column_fields_from_scooper]

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
        for i in range(0, history_size, PostUtils.POSTS_NUM):
            from_i = i
            to_i = i + PostUtils.POSTS_NUM
            post_size = len(post_ids_to_update)
            if to_i > post_size >= from_i:
                to_i = min(i + PostUtils.POSTS_NUM, post_size)
            elif to_i > post_size:
                to_i = min(i + PostUtils.POSTS_NUM, history_size)
                from_i = to_i
            post_id_slice = post_ids_to_update[from_i:to_i]
            post_history_id_slice = history_ids_to_update[from_i:to_i]
            message = self.get_message(post_history_id_slice, post_id_slice)
            messages.append(message)
        return messages

    @staticmethod
    def extract_platform_name(x):
        if x is not None:
            media_name = x.split("/")[2].split('.')[0]
            if str.lower(media_name) not in CreatorUtils.valid_platforms:
                return None
            else:
                return str.upper(media_name)

        # post_id_values_to_update = list(new_posts[PostUtils.POST_ID])
        # post_history_id_values_to_update = list(self.df_data[PostUtils.POST_HISTORY_ID])
        # return post_id_values_to_update, post_history_id_values_to_update
