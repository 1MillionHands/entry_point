# Third-party libraries
import pandas as pd

# Local application/library specific imports
from DB_Manager_EP.DB_alchemy import create_local_engine, DbService
from utils import *
from shared.connectors.s3_connector import S3Connector
from shared.connectors.sqs_connector import SQSConnector



with open('DB_Manager_EP/config_file.json', 'r') as f:
    config_data = json.load(f)


class TableHandler:

    def __init__(self, data, event):
        self.data = data
        self.data_df = pd.DataFrame(data)
        self.event = event
        engine = create_local_engine(event['env_status'])
        self.db_obj = DbService(engine)
        # from DB_Manager_EP.db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory
        # existing_creators = self.db_obj.filter_query_table(Creatort, Creatort.name, ['Israel','gomaa1130'],to_df=True)
        self.q = SQSConnector(config_data['sqs']['queue_url'],
                              access_key=config_data['sqs']['access_key'],
                              secret_key=config_data['sqs']['secret_key'])

    def run(self):
        pass

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

    def send_messages(self, messages):

        for m in messages:
            m = json.dumps(m)
            response = self.q.send_message(m)
            print(f"Sent Message: {response['MessageId']}")

    @staticmethod
    def columns_exist_in_external_data(db_columns, external_cols):
        return [col for col in db_columns if col in external_cols]