# Third-party libraries
import pandas as pd
import json

# Local application/library specific imports
from DB_Manager_EP.DB_alchemy import DbService

from DB_Manager_EP.connectors.s3_connector import S3Connector
from DB_Manager_EP.connectors.sqs_connector import SQSConnector


class TableHandler:

    def __init__(self, event):
        self.json_data = None
        self.df_data = pd.DataFrame()
        self.event = event
        self.db_obj = DbService(event['test_env_status'])
        self.q = SQSConnector()
        self.s3object = S3Connector()

    def run(self, run_type=None):
        print("Extracting data...")
        self.extract_data()
        print("Validating data...")
        self.validate_date()
        print("Transforming data...")
        self.transform()
        print("Inserting data into db...")
        self.update_db_insert()

    def upload_to_s3(self, filename):
        try:
            s3 = S3Connector()
            s3.write_raw_posts(self.data)
        except Exception as e:
            print("FAILED", e)

    def update_db_insert(self, tbl_object=None, records=None):
        pass

    def update_db_delete_insert(self, tbl_object, records, keys, id_lst):
        """
        Updates the database by inserting new creators and posts, returning IDs for updates.
        :return: tuple (list, list) of post ID values to update and post history ID values to update
        """
        self.db_obj.delete_table(tbl_object, keys, id_lst)
        self.db_obj.insert_table(tbl_object, records)

    def extract_data(self):
        pass

    def validate_date(self):
        pass

    def transform(self):
        pass

    def send_messages(self, messages):

        for m in messages:
            m = json.dumps(m)
            response = self.q.send_message(m)
            print(f"Sent Message: {response['MessageId']}")

    @staticmethod
    def columns_exist_in_external_data(db_columns, external_cols):
        return [col for col in db_columns if col in external_cols]