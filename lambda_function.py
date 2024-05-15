
def lambda_handler(event, context):
    if event['env'] == 'dev':
        print("success")
    else:
        pass
