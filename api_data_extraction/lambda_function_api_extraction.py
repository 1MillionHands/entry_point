import sys
from table_objects.events_handler import EventHandler

sys.path.append('opt/site-packages')

def lambda_handler(event, context):
    # data = None
    #
    print("the original event", event)
    custom_payload = compile_event_payload(event)
    if custom_payload == None:
        raise Exception("Invalid event structure")

    if custom_payload["test_env_status"]:
        obj = EventHandler()
        pyload = obj.run(custom_payload)
    else:
        print("success")
    return {
        'statusCode': 200,  # HTTP status code
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': pyload
    }
