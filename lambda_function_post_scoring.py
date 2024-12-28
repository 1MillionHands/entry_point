import sys
from data_enrichment import scoring

sys.path.append('opt/site-packages')

def compile_event_payload(event):
    body =  event.get('body',None)
    if body is not None:
        print('the body the run got is ', body)
        event = body
    else:
        event = None

    return event


def lambda_handler(event, context):
    # data = None
    #
    print("the original event", event)
    custom_payload = compile_event_payload(event)

    if custom_payload == None:
        raise Exception("Invalid event structure")

    env_msg = 'test' if custom_payload["test_env_status"] else 'prod'
    print(f'The program running in {env_msg} mode')

    msg = scoring.run(custom_payload)

    return msg
