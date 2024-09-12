import uuid

import numpy as np
# Third-party libraries
import pandas as pd

# Local application/library specific imports
from DB_Manager_EP.db_table_objects import Creatort, CreatorHistoryt
from data_sources.scooper.table_object import ScooperRowData
from table_objects.utils import CreatorUtils
from DB_Manager_EP.table_handler import TableHandler


class CreatorHandler(TableHandler):

    def __init__(self, event):
        super().__init__(event)
        self.timestamp_partition_id = event[CreatorUtils.TIMESTAMP_PARTITION_ID]

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

        self.query_raw_data()
        creator, creator_history, regen_creator_id = self.transform()

        if creator is not None and creator_history is not None:
            # update creators
            self.update_db_insert(Creatort, creator)

            # update creators history
            self.update_db_delete_insert(CreatorHistoryt, creator_history, CreatorUtils.INGESTION_TIMESTAMP_FIELD,
                                         [self.timestamp_partition_id])

    def run_from_circle(self):
        pass

    def query_raw_data(self):
        filters = [
            {
                # "column": utils.CreatorINGESTION_TIMESTAMP_FIELD,
                "column": "ingestion_timestamp",
                "values": [self.timestamp_partition_id],
                "op": "in"
            }
        ]
        self.df_data = self.db_obj.query_table_orm(ScooperRowData, filters=filters, distinct=True, to_df=True,
                                                   columns=CreatorUtils.query_raw_data_fields)[0]

    print("here")
    print("here2")

    def transform(self):

        self.df_data['platform_name'] = self.df_data.media_url.apply(lambda x: self.extract_platform_name(x))
        self.df_data.drop(columns='media_url', inplace=True)
        self.df_data.drop_duplicates(subset=CreatorUtils.primary_key, inplace=True)

        # Drop nulls rows
        self.df_data = self.df_data.dropna(how='all')
        self.df_data = self.df_data.dropna(subset=['creator_name', 'platform_name'])

        # Use get all the ids, name and platform name from new df, and query creator table using filter query table, filter name and platform name
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

        # The query_table_orm fun return ( df, invalid columns), so we take df that located in index 0
        # existing_creators = self.db_obj.query_table_orm(table_name=ScoperTemp, filters=filters, distinct=True, to_df=True)[0]
        existing_creators = \
        self.db_obj.query_table_orm(table_name=Creatort, filters=filters, distinct=True, to_df=True)[0]

        if existing_creators is not None:
            self.df_data = self.df_data.merge(existing_creators, how='left', on=['creator_name', 'platform_name'])

            # Check if new creators has id
            map_id_doesnt_exist = self.df_data[['creator_id_x']].isnull()
            map_is_new_creator = self.df_data[['creator_id_y']].isnull()

            self.df_data['creator_id_x'] = self.set_creator_id(map_id_doesnt_exist, map_is_new_creator)

            self.df_data.rename(columns={'creator_id_x': 'creator_id', 'sentiment_score_x': 'sentiment_score',
                                         'creator_image_x': 'creator_image', 'creator_url_x': 'creator_url',
                                         'language_x': 'language'}, inplace=True)

            self.df_data.drop(columns=['sentiment_score_y', 'creator_image_y', 'creator_url_y', 'language_y'],
                              inplace=True)

        self.df_data[CreatorUtils.CREATOR_HISTORY_ID] = [str(uuid.uuid4()) for i in range(self.df_data.shape[0])]

        creator_current_cols = self.columns_exist_in_external_data(CreatorUtils.CREATOR_FIELDS,
                                                                   self.df_data.columns)
        if existing_creators is not None:
            creators = self.df_data[self.df_data[CreatorUtils.CREATOR_ID_YEAR].isnull()][creator_current_cols]
        else:
            creators = self.df_data[creator_current_cols]

        # Correct null values by type
        creators = creators.replace(np.nan, None)
        creators = creators.replace({pd.NaT: None})
        creators = creators.to_dict(orient='records')

        creator_history_current_cols = self.columns_exist_in_external_data(CreatorUtils.CREATOR_HISTORY_VARIABLES,
                                                                           self.df_data.columns)
        creators_history = self.df_data[creator_history_current_cols].to_dict(orient='records')
        return creators, creators_history


    @staticmethod
    def set_null_value(value):
        import math
        if isinstance(value, (int, float)):
            return math.nan
        else:
            return None

    def set_creator_id(self, map_id_doesnt_exist, map_is_new_creator):
        id_lst = []
        for new_current_id, current_id, id_doesnt_exist, is_new_creator in zip(self.df_data['creator_id_x'].values,
                                                                               self.df_data['creator_id_y'].values,
                                                                               map_id_doesnt_exist.values,
                                                                               map_is_new_creator.values):

            if id_doesnt_exist and is_new_creator:
                id_lst.append(str(uuid.uuid4()))
            elif id_doesnt_exist and not is_new_creator:
                id_lst.append(current_id)
            else:
                id_lst.append(new_current_id)
        return id_lst

    @staticmethod
    def extract_platform_name(x):
        if x is not None:
            media_name = x.split("/")[2].split('.')[0]
            if str.lower(media_name) not in CreatorUtils.valid_platforms:
                return None
            else:
                return str.upper(media_name)

    def update_db_insert(self, tbl_object=None, records=None):
        """
        Updates the database by inserting new creators and posts, returning IDs for updates.
        :return: tuple (list, list) of post ID values to update and post history ID values to update
        """
        self.db_obj.insert_table(tbl_object, records)

    def update_db_delete_insert(self, tbl_object, records, keys, id_lst):
        """
        Updates the database by inserting new creators and posts, returning IDs for updates.
        :return: tuple (list, list) of post ID values to update and post history ID values to update
        """
        self.db_obj.delete_table(tbl_object, keys, id_lst)
        self.db_obj.insert_table(tbl_object, records)

    def preprocess_posts_to_fit_db(self):
        """
        Preprocesses creator data to ensure compatibility with database constraints and rules.
        :return: List of dictionaries with preprocessed post data
        """
