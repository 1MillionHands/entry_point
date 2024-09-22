# exnternal libraries
import requests
import boto3
import datetime
import json
import os

# local libraries
from DB_Manager_EP.connectors import s3_connector
from api_data_extraction.util import ApiUtil

# Get the absolute path to the current script
current_dir = os.path.dirname(__file__)

# Construct the path to the config file relative to the current script
config_path = os.path.join(current_dir, '../config_file.json')

# Normalize the path (handles symbolic links, etc.)
config_path = os.path.normpath(config_path)
#
# # Open and read the config file
with open(config_path, 'r') as f:
    config_data = json.load(f)


class ExtractScooperData:

    def __init__(self, event):
        self.s3_connector = s3_connector.S3Connector()
        self.event = event

        if event.get('source_url_list', 'None') != 'None':
            self.source_url_list = event["source_url_list"]
            self.bucket_name = event['bucket_name']
            self.key_prefix = event['key_prefix']
            self.output_key = event['output_key']
        else:
            self.source_url_list = ApiUtil.source_url_list
            self.bucket_name = ApiUtil.bucket_name
            self.key_prefix = ApiUtil.key_prefix
            self.output_key = ApiUtil.output_key

        self.env = self.validate_env(event['test_env_status'])
        self.source_url_dict = self.set_source_utl_list()

    @staticmethod
    def validate_env(test_env_status):
        if test_env_status in ["test", "prod"]:
            return test_env_status
        else:
            raise Exception("test_env_status must be 'test' or 'prod'")

    def set_source_utl_list(self):
        dict_ = {}

        for url in self.source_url_list:
            url_res = ApiUtil.source_url_dict.get(url, 'None')
            if url_res == 'None':
                raise Exception("given key wasn't found in the class util 'source_url_dict'")
            else:
                dict_[url] = url_res
            return dict_

    def run(self):
        # Create an S3 client
        s3 = boto3.client("s3")

        print('the is the source url dict', self.source_url_dict)

        # Iterate over the URLs and upload the data to S3
        for name, url in self.source_url_dict.items():
            print(f"Uploading {name} data to S3...")
            # Fetch the JSON data from the URL
            response = requests.get(url)
            data = response.json()

            # extract current data, and string it as part of the output key - is_test\year\month\day\name
            current_date = datetime.datetime.now()
            formatted_datetime = current_date.strftime('%Y-%m-%d-%H-%M-%S')

            # Create a file name based on the URL name
            file_name = f"{formatted_datetime}_scooper.json"

            key_prefix = f"{self.env}/{self.output_key}/{current_date.year}/{current_date.month}/{current_date.day}/{name}"
            bucket_name = self.bucket_name

            # Upload the data to S3
            s3.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=f"{key_prefix}/{file_name}")

            print(f"Uploaded {name}/{file_name} to S3")

# if __name__ == "__main__":
#     event = {
#         "test_env_status": False
#     }
#     obj = ExtractScooperData(event)
#     obj.run()
