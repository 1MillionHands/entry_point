import boto3
import json
import os

# Get the absolute path to the current script
current_dir = os.path.dirname(__file__)

# Construct the path to the config file relative to the current script
config_path = os.path.join(current_dir, '../../config_file.json')

# Normalize the path (handles symbolic links, etc.)
config_path = os.path.normpath(config_path)

# Open and read the config file
with open(config_path, 'r') as f:
    config_data = json.load(f)


class S3Connector:
    def __init__(self):
        self.s3 = boto3.resource("s3", region_name=config_data["s3"]["region_name"])

    # upload cont to s3 bucket
    def _upload_file(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name
        try:
            response = self.s3.upload_file(self, file_name, bucket, object_name)
        except Exception as e:
            print(e)
            return False
        return True

    def _read_file_from_s3(self, bucket, key):

        try:
            response = self.s3.Object(bucket, key)
            return response
        except Exception as e:
            print(e)
            return False

    def _read_file(self, file_name, bucket=None):

        try:
            bucket = self.bucket_name if self.bucket_name is not None else bucket
            response = self.s3.get_object(Bucket=bucket, Key=file_name)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            print(e)
            return False

    def _put_object(self, object_data, bucket, key):
        s3_client = self.s3.meta.client
        try:
            response = s3_client.put_object(Bucket=bucket, Body=object_data, Key=key)
            return response
        except Exception as e:
            print(e)
            return False

    @staticmethod
    def get_secret():
        secret_name = config_data["secret"]["secret_name"]
        region_name = config_data["secret"]["region_name"]

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except Exception as e:
            print(f"Error retrieving secret: {e}")
            raise e

        # Decrypts secret using the associated KMS key.
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)

    # def read_posts(self):
    #     return self._read_file(self.input_file, self.bucket_name)
    #
    # def write_posts(self, posts, archive=False):
    #     """posts should be json string"""
    #     self._put_object(posts, self.bucket_name, self.output_file)
    #     if archive:
    #         self.archive_posts(posts)
    #
    # # function to write posts to "archive" folder in the s3 bucket
    # def archive_posts(self, posts):
    #     """posts should be json string"""
    #     short_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    #     self._put_object(posts, self.bucket_name, f'{self.archive_folder}/scraped_{short_timestamp}.json')
    #
    # def write_raw_posts(self, posts):
    #     """posts should be json string"""
    #     self._put_object(posts, self.bucket_name, self.input_file)

if __name__ == "__main__":
# from db_table_objects import Post, Creatort, CreatorHistoryt, PostHistory,Volunteer, Base
#

    # Get the absolute path to the current script
    current_dir = os.path.dirname(__file__)
    print("1",current_dir)

    # Construct the path to the config file relative to the current script
    config_path = os.path.join(current_dir, '../config_file.json')
    print("3",config_path)

    # Normalize the path (handles symbolic links, etc.)
    config_path = os.path.normpath(config_path)
    print("1",config_path)

    # is_test=False
    # ob = DbService(is_test)
    print("success")