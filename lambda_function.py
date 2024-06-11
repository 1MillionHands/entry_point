from events_handler import EventHandler
import sys
sys.path.append('opt/site-packages')


def lambda_handler(event, context):
    if event['env'] == 'dev':
        EventHandler().run(event)
    else:
        print("success")

if __name__ == '__main__':
    lambda_handler({'env': 'de'}, '')
