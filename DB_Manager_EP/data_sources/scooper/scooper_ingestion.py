# Third-party libraries
import pandas as pd
import numpy as np
import uuid

import json
from DB_Manager_EP.data_sources.scooper.table_object import ScooperRowData, Utils
from DB_Manager_EP.table_handler import TableHandler
from datetime import datetime
from pytz import timezone

JSON_ENTRY_KEY = 'result_attributes'
DATATIME_LIST = ['search_indexed_datetime', 'indexed_datetime', 'published_datetime']


class ScooperIngestion(TableHandler):
    def __init__(self, event):
        super().__init__(event)
        self.df_data = None
        event = self.validate_event(event)
        self.key = self.set_input_file(event)
        self.bucket = event['bucket_name']
        self.table_object = ScooperRowData
        tz = timezone('EST')
        curr_timestamp = datetime.now(tz)
        self.running_timestamp_id = curr_timestamp
        # self.running_timestamp_id = None

    @staticmethod
    def validate_event(event,):
        if any(field not in event for field in ['input_file', 'bucket_name']):
            raise Exception("input_file, bucket_name and output_file are required")
        if any(event[field] is None for field in ['input_file', 'bucket_name']):
            raise Exception("any of input_file, bucket_name values are empty")

        return event

    @staticmethod
    def set_input_file(event):
        if "scooper" not in event['input_file']:
            raise Exception("scooper is not in event")
        return event['input_file']

    def extract_data(self):

        obj = self.s3object._read_file_from_s3(self.bucket, self.key)
        if obj:
            data = obj.get()['Body'].read().decode('utf-8')
            self.json_data = json.loads(data)

        else:
            raise Exception("Error reading data from s3 - could be file is empty")

    def transform_date(self):
        print("Transforming data...")
        entries = [entry[JSON_ENTRY_KEY] for entry in self.json_data[0]['entries']]
        # self.df_data = pd.DataFrame(self.json_data[0])

        self.df_data = pd.DataFrame(entries)
        self.set_new_columns()

        self.df_data[DATATIME_LIST] = self.transform_datetime_cols(self.df_data[DATATIME_LIST])

        string_na_fields = [field for field in self.df_data.columns if field not in Utils.NUMERIC_FIELDS]
        self.df_data[string_na_fields] = self.df_data[string_na_fields].replace({np.nan: None})
        self.df_data['id'] = [str(str(uuid.uuid4())) for i in range(self.df_data.shape[0])]

        tz = timezone('EST')
        curr_timestamp = datetime.now(tz)
        self.df_data['ingestion_timestamp'] = curr_timestamp
        self.running_timestamp_id = curr_timestamp

    # insert data base
    def update_db(self):
        print("Inserting data into db...")
        try:
            if not self.db_obj.check_table_exists(ScooperRowData.__tablename__):
                raise Exception(f"table {self.table_object} currently not exists in the data base")
            records_data = self.df_data.to_dict(orient='records')
            self.db_obj.insert_table(tbl_obj=self.table_object, headers=records_data)
            print("Done inserting data to the database")

        except Exception as e:
            print("Error ", e)

    def set_new_columns(self):
        # Get the input file columns, and rename them according to the table object fields conventions
        new_column_names = [ScooperIngestion.rename_column(col) for col in self.df_data.columns]
        # Get all the columns from the table objects
        self.df_data.columns = new_column_names

        # In case fields doesn't exist in the input file, get all missing fields and add None into the missing columns
        missing_columns = list(set(Utils.INGESTION_COLUMN_NAMES.values()) - set(new_column_names))
        if missing_columns:
            for col in missing_columns:
                if col in Utils.NUMERIC_FIELDS:
                    self.df_data[col] = np.nan
                else:
                    self.df_data[col] = None

    @staticmethod
    def rename_column(col):
        try:
            return Utils.INGESTION_COLUMN_NAMES[col]
        except KeyError as e:
            print(
                f"Error in finding the column in the database table object columns list - {col} doesn't exist. \n Error - {e}")

    @staticmethod
    def transform_datetime_cols(df):
        df = df.applymap(lambda x: x.replace('/', '-') if pd.notnull(x) else x)

        for col in df.columns:
            df[col] = pd.to_datetime(df[col])
        return df
