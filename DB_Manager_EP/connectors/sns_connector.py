import json
import boto3

REGION = "eu-north-1"
class SNSConnector:
    def __init__(self, topic_arn=None, region=REGION, from_service=None, to_service=None, access_key=None, secret_key=None):
        self.from_service = from_service
        self.to_service = to_service
        self.region = region
        self.topic_arn = topic_arn or 'arn:aws:sns:{}:976476826599:{}_{}'.format(self.region, self.from_service, self.to_service)
        self.access_key = access_key
        self.secret_key = secret_key
        self.sns = boto3.client('sns', aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key, region_name=self.region)

    def publish_message(self, message, subject=None):
        response = self.sns.publish(
            TopicArn=self.topic_arn,
            Message=message,
            Subject=subject
        )
        return response

if __name__ == '__main__':
    with open('config_file_.json', 'r') as f:
        config_data = json.load(f)
    # Example Usage:
    # Replace 'your_topic_arn' with your actual SNS topic ARN
    topic_arn = 'arn:aws:sns:eu-north-1:976476826599:Scrapers_Scraped'
    sns_connector = SNSConnector(topic_arn, access_key=config_data['sqs']['access_key'], secret_key=config_data['sqs']['secret_key'])

    # Publishing a message
    message = 'Hello, SNS!'
    subject = 'Test Subject'
    response = sns_connector.publish_message(message, subject)
    print(f"Published Message: {response['MessageId']}")