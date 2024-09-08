from events_handler import EventHandler
import sys
sys.path.append('opt/site-packages')


def lambda_handler(event, context):
    # data = None
    #
    # print("the original event", event)
    # custom_payload = compile_event_paylaod(event)
    custom_payload = event
    if custom_payload["test_env_status"]:
        obj = EventHandler()
        data = obj.run(custom_payload)
        print("")
    else:
        print("success")

    if data:
        return {
            'statusCode': 200,  # HTTP status code
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': data.running_timestamp_id # Body must be a string (usually JSON)
        }
