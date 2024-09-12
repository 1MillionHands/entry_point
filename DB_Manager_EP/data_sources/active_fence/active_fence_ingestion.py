# Third-party libraries
import pandas as pd
import gzip
import uuid
from io import BytesIO

# Local application/library specific imports
from DB_Manager_EP.data_sources.active_fence.table_object import ActiveFenceRowData
from DB_Manager_EP.table_handler import TableHandler
from utils import *


class ActiveFenceIngest(TableHandler):
    def __init__(self, event):
        super().__init__(event)
        self.df_data = None
        self.key = event['input_file']
        self.bucket = event['bucket_name']
        self.output_file_name = event['output_file']
        self.table_object = ActiveFenceRowData

    def extract_data(self):

        obj = self.s3object._read_file_from_s3(self.bucket, self.key)

        with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
            content = gzipfile.read()
            gzipfile.close()
        self.json_data = content
        print("Done reading data from s3")


    def transform_date(self):
        self.df_data = pd.read_json(BytesIO(self.json_data), lines=True)
        self.df_data.drop(columns='coauthor_producers', inplace=True)
        self.df_data['id'] = [str(str(uuid.uuid4())) for i in range(self.df_data.shape[0])]
        self.df_data.has_handshake.fillna(False, inplace = True)
        self.df_data = self.df_data.iloc[0:1000,:]

    # insert data base
    def update_db_insert(self):
        try:
            if not self.db_obj.check_table_exists(ActiveFenceRowData.__tablename__):
                raise Exception(f"table {self.table_object} currently not exists in the data base")
            records_data = self.df_data.to_dict(orient='records')
            self.db_obj.insert_table(tbl_obj=self.table_object, headers=records_data)
            print("Done inserting data to the database")

        except Exception as e:
            print("Error ", e)

































