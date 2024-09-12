from events_handler import EventHandler
import sys

sys.path.append('opt/site-packages')


def extract_s3_info(event):
    # Check if the event structure is as expected
    try:
        # The S3 event is in 'Records' -> first record -> 's3' -> 'bucket' and 'object'
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        event_name = key.split('/').split('.')[0]
        test_env_status = key.split('/')[0] == 'test'

        new_event = {
            'event_name': event_name
            , 'bucket_name': bucket
            , 'input_file': 'scooper_imports/2024/09/08/scooper.json'
            , 'test_env_status': test_env_status
            , 'output_file': ''
        }

        return new_event

    except KeyError as e:
        print(f"KeyError - the expected field is not present in the event: {e}")
        return None, None


def compile_event_payload(event):
    if event['body']['temp'] is not None:
        event['body'] = event['temp']
    elif event['Records'] is not None:
        event = extract_s3_info(event)
    else:
        event = None

    return event


def lambda_handler(event, context):
    # data = None
    #
    # print("the original event", event)
    custom_payload = compile_event_payload(event)
    if custom_payload == None:
        raise Exception("Invalid event structure")

    if custom_payload["test_env_status"]:
        obj = EventHandler()
        pyload = obj.run(custom_payload)
        print("")
    else:
        print("success")

    if pyload:
        return {
            'statusCode': 200,  # HTTP status code
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': pyload
        }
