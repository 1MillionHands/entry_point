import sys
from table_objects.events_handler import EventHandler

sys.path.append('opt/site-packages')


def compile_event_payload(event):
    if 'responsePayload' in list(event.keys()):
        if 'event' in list(event['responsePayload'].keys()):
            if event['responsePayload']['event'] is not None:
                event_ = event['responsePayload']['event']

                new_event = {
                    'event_name': event_['event_name']
                    , 'test_env_status': event_['test_env_status']
                    , 'id': event_['id']
                }

                return new_event

    else:
        new_event = None
        return new_event


def lambda_handler(event, context):
    # data = None
    #
    print("the original event", event)
    custom_payload = compile_event_payload(event)
    if custom_payload == None:
        raise Exception("Invalid event structure")

    env_msg = 'test' if custom_payload["test_env_status"] else 'prod'
    print(f'The program running in {env_msg} mode')

    obj = EventHandler()
    obj.run(custom_payload)
    return {
        'statusCode': 200,  # HTTP status code
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': custom_payload
    }
