import boto3
from datetime import datetime



# ACCESS_KEY = get_config()['s3']['access_key']
# SECRET_KEY = get_config()['s3']['secret_key']


class S3Connector:
    def __init__(self,  access_key, secret_key, bucket_name='onemillionhands',
                 input_file='raw_posts_list.csv', output_file='scraped_posts.json',
                 archive_folder='archive'):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.input_file = input_file
        self.output_file = output_file
        self.archive_folder = archive_folder


    # upload cont to s3 bucket
    def _upload_file(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        try:
            response = s3_client.upload_file(self, file_name, bucket, object_name)
        except Exception as e:
            print(e)
            return False
        return True


    def _read_file(self, file_name, bucket):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        try:
            response = s3_client.get_object(Bucket=bucket, Key=file_name)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            print(e)
            return False


    def _put_object(self, object_data, bucket, key):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        try:
            response = s3_client.put_object(Bucket=bucket, Body=object_data, Key=key)
            return response
        except Exception as e:
            print(e)
            return False
        


    def read_posts(self):
        return self._read_file(self.input_file, self.bucket_name)

    def write_posts(self, posts, archive=False):
        """posts should be json string"""
        self._put_object(posts, self.bucket_name, self.output_file)
        if archive:
            self.archive_posts(posts)

    # function to write posts to "archive" folder in the s3 bucket
    def archive_posts(self, posts):
        """posts should be json string"""    
        short_timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        self._put_object(posts, self.bucket_name, f'{self.archive_folder}/scraped_{short_timestamp}.json')

    def write_raw_posts(self, posts):
        """posts should be json string"""
        self._put_object(posts, self.bucket_name, self.input_file)