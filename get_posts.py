import requests,json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from onemilshared.connectors.s3_connector import S3Connector
from schedule import every, repeat, run_pending
from time import sleep

with open('config_file.json', 'r') as f:
  config_data = json.load(f)

headers = {"accept": "application/json",
           "Cookie": f"CF_Authorization={config_data['letBotsWorkToekn']}"}
page_size = config_data.get("page_size")

retry_strategy = Retry(
    total=10,  # Maximum number of retries
    status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
)
adapter = HTTPAdapter(max_retries=retry_strategy)


session = requests.Session()
session.mount('http://', adapter)
session.mount('https://', adapter)

@repeat(every(config_data['schedule']['hours']).hours)
def get_posts():
    print("getting posts")
    response = session.get(f'https://api.oct7.io/posts?sort=created_at.desc&limit={page_size}',headers=headers, timeout=50)

    if response.status_code == 200:
        raw_posts = response.text
        s3 = S3Connector(access_key=config_data['s3']['access_key'], secret_key=config_data['s3']['secret_key'],
                        input_file='raw_posts.json')
        p = json.loads(raw_posts)['results']

        s3.write_raw_posts(raw_posts)
        print("SUCCESS")
    else:
        print("FAILED")


print(f"initating scheduler every {config_data['schedule']['hours']} hours")

while True:
    run_pending()
    sleep(5)
