import json
import os.path

import boto3
### Add to config file
REGION = "eu-north-1"
class SQSConnector:
    def __init__(self, queue_url, access_key=None , secret_key=None, region_name=REGION):

        self.region_name=region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.sqs = boto3.client('sqs',aws_access_key_id=self.access_key,  aws_secret_access_key=self.secret_key,
                                region_name=self.region_name)
        self.queue_url = queue_url

    def send_message(self, message_body):
        response = self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_body
        )
        return response

    def receive_messages(self, num_messages=1, visibility_timeout=30):
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=num_messages,
            MessageAttributeNames=['All'],
            VisibilityTimeout=visibility_timeout,
            WaitTimeSeconds=0
        )

        messages = response.get('Messages', [])
        return messages

    def delete_message(self, receipt_handle):
        self.sqs.delete_message(
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle
        )
if __name__ == '__main__':
    #Add the region setting!
    queue_url = 'https://sqs.eu-north-1.amazonaws.com/976476826599/Scrapers_DSservice_Scoring'
    config_file_path = 'config_file_.json'
    if (os.path.exists(config_file_path)):
        with open('config_file_.json', 'r') as f:
            config_data = json.load(f)

    sqs = SQSConnector(queue_url, access_key=config_data['sqs']['access_key'], secret_key=config_data['sqs']['secret_key'])
    message_body = 'Hello, SQS!'
    response = sqs.send_message(message_body)
    print(f"Sent Message: {response['MessageId']}")

    # Receiving messages
    messages = sqs.receive_messages(num_messages=1, visibility_timeout=30)
    for message in messages:
        print(f"Received Message: {message['Body']}")
        # Process the message

        # Deleting the processed message
        sqs.delete_message(message['ReceiptHandle'])
        print(f"Deleted Message: {message['MessageId']}")

# Example Usage:
# Replace 'your_queue_url' with your actual SQS queue URL

#sqs_connector = SQSConnector(queue_url)
"""
# Sending a message
message_body = 'Hello, SQS!'
response = sqs_connector.send_message(message_body)
print(f"Sent Message: {response['MessageId']}")



# Receiving messages
messages = sqs_connector.receive_messages(num_messages=5, visibility_timeout=30)
for message in messages:
    print(f"Received Message: {message['Body']}")
    # Process the message

    # Deleting the processed message
    sqs_connector.delete_message(message['ReceiptHandle'])
    print(f"Deleted Message: {message['MessageId']}")
    
    """