from events_handler import EventHandler


def lambda_handler(event, context):
    if event['env'] == 'dev':
        EventHandler().run(event)
    else:
        print("success")

# if __name__ == '__main__':
#     lambda_handler({'env': 'de'}, '')
