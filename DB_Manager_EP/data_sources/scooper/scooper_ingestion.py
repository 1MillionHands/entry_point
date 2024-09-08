# Third-party libraries
import pandas as pd
import numpy as np
import uuid

import json
from DB_Manager_EP.data_sources.scooper.table_object import ScooperRowData, Utils
from DB_Manager_EP.table_handler import TableHandler

JSON_ENTRY_KEY = 'result_attributes'


class ScooperIngestion(TableHandler):
    def __init__(self, event):
        super().__init__(event)
        self.df_data = None
        self.key = event['input_file']
        self.bucket = event['bucket_name']
        self.output_file_name = event['output_file']
        self.table_object = ScooperRowData

    def extract_data(self):

        obj = self.s3object._read_file_from_s3(self.bucket, self.key)
        if obj:
            data = obj.get()['Body'].read().decode('utf-8')
            self.json_data = json.loads(data)
            print('')
        else:
            return None


    def transform_date(self):
        print("Transforming data...")
        entries = [entry[JSON_ENTRY_KEY] for entry in self.json_data[0]['entries']]
        # self.df_data = pd.DataFrame(self.json_data[0])

        self.df_data = pd.DataFrame(entries)
        self.set_new_columns()

        self.df_data[['search_indexed_datetime', 'indexed_datetime', 'published_datetime']] = self.trasnform_datetime_cols(['search_indexed_datetime', 'indexed_datetime', 'published_datetime'])


        string_na_fields = [field for field in self.df_data.columns if field not in Utils.NUMERIC_FIELDS]
        self.df_data[string_na_fields] = self.df_data[string_na_fields].replace({np.nan: None})
        self.df_data['id'] = [str(str(uuid.uuid4())) for i in range(self.df_data.shape[0])]



    # insert data base
    def update_db(self):
        try:
            if not self.db_obj.check_table_exists(ScooperRowData.__tablename__):
                raise Exception(f"table {self.table_object} currently not exists in the data base")
            records_data = self.df_data.to_dict(orient='records')
            self.db_obj.insert_table(tbl_obj=self.table_object, headers=records_data)
            print("Done inserting data to the database")

        except Exception as e:
            print("Error ", e)

    def set_new_columns(self):
        new_column_names = [ScooperIngestion.rename_column(col) for col in self.df_data.columns]
        self.df_data.columns = new_column_names

        missing_columns = list(set(Utils.INGESTION_COLUMN_NAMES.values()) - set(new_column_names))
        if missing_columns:
            for col in missing_columns:
                self.df_data[col] = None


    @staticmethod
    def rename_column(col):
        try:
            return Utils.INGESTION_COLUMN_NAMES[col]
        except KeyError as e:
            print(f"Error in finding the column in the database table object columns list - {col} doesn't exist. \n Error - {e}")

    @staticmethod
    def trasnform_datetime_cols(df, cols):
        df[cols] = df[[cols]].applymap(lambda x: x.replace('/', '-') if pd.notnull(x) else x)
        for col in cols:
            df[col] = pd.to_datetime(df[col])
        return df
