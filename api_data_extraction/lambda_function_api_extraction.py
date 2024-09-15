import sys
from api_data_extraction import main

sys.path.append('opt/site-packages')


def lambda_handler(event, context):
    print("the original event", event)

    obj = main.ExtractScooperData(event)
    obj.run()
