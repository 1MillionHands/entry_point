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
        creator, creator_history = self.transform()

        if creator is not None:
            # update creators
            print(f"inserting {len(creator)} amount of rows in creator")
            self.update_db_insert(Creatort, creator)

        if creator_history is not None:
            # update creators history
            print(f"inserting {len(creator_history)} amount of rows in creator_history")
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

    def transform(self):

        self.df_data.dropna(subset=['media_url'], inplace=True)
        self.df_data['platform_type'] = self.df_data.media_url.apply(lambda x: self.extract_platform_name(x))
        self.df_data.drop(columns='media_url', inplace=True)
        self.df_data.drop_duplicates(subset=CreatorUtils.primary_key, inplace=True)

        # Drop nulls rows
        self.df_data = self.df_data.dropna(how='all')
        self.df_data = self.df_data.dropna(subset=CreatorUtils.primary_key)

        # Use get all the ids, name and platform name from new df, and query creator table using filter query table, filter name and platform name
        filters = [
            {
                "column": "name",
                "values": list(self.df_data.name)
            },
            {
                "column": "platform_type",
                "values": list(self.df_data.platform_type)
            }
        ]

        # The query_table_orm fun return ( df, invalid columns), so we take df that located in index 0
        # existing_creators = self.db_obj.query_table_orm(table_name=ScoperTemp, filters=filters, distinct=True, to_df=True)[0]
        existing_creators = \
        self.db_obj.query_table_orm(table_name=Creatort, filters=filters, distinct=True, to_df=True)[0]
        creators = self.set_creator_dims(existing_creators)

        creators_history = self.set_create_history_id(creators[['creator_id', 'name', 'platform_type', 'indicator']])

        creators = creators[creators.indicator]
        creators.drop(columns=['indicator'], inplace=True)
        creators = creators.to_dict(orient='records')
        return creators, creators_history

    def set_creator_dims(self, existing_creators):
        if existing_creators is None:
            new_creators = self.df_data.copy()
            new_creators['creator_id'] = [str(uuid.uuid4()) for i in range(new_creators.shape[0])]
            new_creators['indicator'] = True
            new_creators = new_creators.copy()
        else:
            existing_creators.platform_type = existing_creators.platform_type.apply(lambda x: x.name)
            existing_creators.drop_duplicates(subset=CreatorUtils.primary_key, inplace=True)
            new_creators = self.df_data.merge(existing_creators, how='left', on=CreatorUtils.primary_key)

            if new_creators['creator_id_y'].isnull().sum() == 0:
                new_creators['creator_id_x'] = new_creators['creator_id_y']
                new_creators['indicator'] = False

            else:
                new_creators_mask = new_creators['creator_id_y'].isnull()
                new_creators['indicator'] = new_creators_mask
                new_creators['creator_id_x'] = new_creators.apply(lambda x: str(uuid.uuid4()) if x['indicator'] else x['creator_id_y'], axis=1)

            new_creators.rename(columns={'creator_id_x': 'creator_id', 'sentiment_x': 'sentiment',
                                         'creator_image_x': 'creator_image', 'creator_url_x': 'creator_url',
                                         'language_x': 'language'}, inplace=True)

            new_creators.drop(columns=['sentiment_y', 'creator_image_y', 'creator_url_y', 'language_y'],
                              inplace=True)

        creator_current_cols = self.columns_exist_in_external_data(CreatorUtils.CREATOR_FIELDS,
                                                                   new_creators.columns)
        creator_current_cols.append('indicator')
        new_creators = new_creators[creator_current_cols]

        # Correct null values by type
        new_creators = new_creators.replace(np.nan, None)
        new_creators = new_creators.replace({pd.NaT: None})
        return new_creators

    def set_create_history_id(self, creators):

        # Create mask to self.df_data that with the creator platform type and name.
        mask = self.df_data['name'].isin(creators['name']) & self.df_data['platform_type'].isin(creators['platform_type'])

        # Set creator id from creators to the masked values of self.df_data
        self.df_data.loc[mask, 'creator_id'] = creators['creator_id']

        self.df_data[CreatorUtils.CREATOR_HISTORY_ID] = [str(uuid.uuid4()) for i in range(self.df_data.shape[0])]

        df_temp = self.df_data.rename(columns={'creator_id': 'creator_fk'}, inplace=False)

        creator_history_current_cols = self.columns_exist_in_external_data(CreatorUtils.CREATOR_HISTORY_VARIABLES,
                                                                           df_temp.columns)
        creators_history = df_temp[creator_history_current_cols].to_dict(orient='records')

        return creators_history

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

    def preprocess_posts_to_fit_db(self):
        """
        Preprocesses creator data to ensure compatibility with database constraints and rules.
        :return: List of dictionaries with preprocessed post data
        """
