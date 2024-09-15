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

        if event.get('source_utl_dict', 'None') != 'None':
            self.source_utl_dict = event["source_utl_dict"]
            self.bucket_name = event['bucket_name']
            self.key_prefix = event['key_prefix']
            self.output_key = event['output_key']
        else:
            self.source_utl_dict = ApiUtil.source_utl_dict
            self.bucket_name = ApiUtil.bucket_name
            self.key_prefix = ApiUtil.key_prefix
            self.output_key = ApiUtil.output_key

        self.env = 'test' if event['test_env_status'] else 'prod'
        self.source_url_dict = self.set_source_url_dict()


        self.is_test = event["test_env_status"]

    def set_source_url_dict(self):
        url_list = self.event.get("source_url_list", 'None')
        dict_ = {}
        if url_list == 'None':
            raise Exception("source_url_list not found in event")
        else:
            url_list = url_list if isinstance(url_list, list) else [url_list]

            for url in url_list:
                url_res = ApiUtil.get(self.source_utl_dict[url], 'None')
                if url_res == 'None':
                    raise Exception("given key wasn't ofund in the class util 'source_utl_dict'")
                else:
                    dict_[url] = url_res
            return dict_


    def run(self):
        # Create an S3 client
        s3 = boto3.client("s3")

        # Iterate over the URLs and upload the data to S3
        for name, url in self.source_url_dict.items():
            # Fetch the JSON data from the URL
            response = requests.get(url)
            data = response.json()

            # exctract current data, and stirng it as part of the output key - is_test\year\month\day\name
            current_date = datetime.datetime.now()

            # Create a file name based on the URL name
            file_name = f"{current_date}_scooper.json"

            key_prefix = f"{self.env}/{self.output_key}/{current_date.year}/{current_date.month}/{current_date.day}/{name}"
            bucket_name = self.bucket_name

            # Upload the data to S3
            s3.put_object(Body=json.dumps(data), Bucket=bucket_name, Key=f"{key_prefix}/{file_name}")

            print(f"Uploaded {file_name} to S3")

# if __name__ == "__main__":
#     event = {
#         "test_env_status": False
#     }
#     obj = ExtractScooperData(event)
#     obj.run()