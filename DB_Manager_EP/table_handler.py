# Third-party libraries
import pandas as pd

# Local application/library specific imports
from DB_Manager_EP.DB_alchemy import DbService
from utils import *
from DB_Manager_EP.connectors.s3_connector import S3Connector
from DB_Manager_EP.connectors.sqs_connector import SQSConnector
import os

# Get the absolute path to the current script
current_dir = os.path.dirname(__file__)

# Construct the path to the config file relative to the current script
config_path = os.path.join(current_dir, '..', 'config_file.json')

with open(config_path, 'r') as f:
    config_data = json.load(f)
with open('./config_file.json', 'r') as f:
  config_data = json.load(f)
# with open(r'C:\Users\yanir\PycharmProjects\oneMilion\entry_point\DB_Manager_EP\config_file_.json', 'r') as f:
#     config_data = json.load(f)



class TableHandler:

    def __init__(self, event):
        self.json_data = None
        self.df_data = pd.DataFrame()
        self.event = event
        self.db_obj = DbService(event['test_env_status'])
        self.q = SQSConnector(config_data['sqs']['queue_url'],
                              access_key=config_data['sqs']['access_key'],
                              secret_key=config_data['sqs']['secret_key'])
        self.s3object = S3Connector(
            access_key=config_data['s3']['access_key']
            , secret_key=config_data['s3']['secret_key']
        )

    def run(self):
        print("Extracting data...")
        self.extract_data()
        print("Validating data...")
        self.validate_date()
        print("Transforming data...")
        self.transform_date()
        print("Inserting data into db...")
        self.update_db()

    def upload_to_s3(self, filename):
        try:
            s3 = S3Connector(access_key=config_data['s3']['access_key'],
                             secret_key=config_data['s3']['secret_key'],
                             input_file=filename)
            s3.write_raw_posts(self.data)
        except Exception as e:
            print("FAILED", e)

    def update_db(self):
        pass

    def extract_data(self):
        pass

    def validate_date(self):
        pass

    def transform_date(self):
        pass

    def send_messages(self, messages):

        for m in messages:
            m = json.dumps(m)
            response = self.q.send_message(m)
            print(f"Sent Message: {response['MessageId']}")

    @staticmethod
    def columns_exist_in_external_data(db_columns, external_cols):
        return [col for col in db_columns if col in external_cols]