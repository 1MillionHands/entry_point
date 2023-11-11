import requests,json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

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



response = session.get(f'https://api.oct7.io/posts?sort=platform.asc&limit={page_size}',headers=headers, timeout=50)

if response.status_code == 200:
    print(response.json())
else:
    print("FAILED")
