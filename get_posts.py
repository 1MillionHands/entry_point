import requests,json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

with open('config_file.json', 'r') as f:
  config_data = json.load(f)

headers = {"accept": "application/json",
           "Cookie": "CF_Authorization=eyJhbGciOiJSUzI1NiIsImtpZCI6ImE3MjQ3ODBmODdmNDY0MmZkYTlhZDM1YjFjYjJkYjBmZjVmMDAyY2JlYzM3MjQyMzY3NDBjZTI0NWJiMGQ4ZmQifQ.eyJhdWQiOlsiMjM4MTAzOWZhYTkxNTdkZTMxZjU1M2U0ZTBhYTQ1OWVlMDY2Mzc1NTgwNzRmM2U2MTA3YWEwNmQ4MmI4MTJkOSJdLCJlbWFpbCI6InYub25lbWlsbGlvbmhhbmRzMUBvY3Q3LmlvIiwiZXhwIjoxNzAxNDU5MDA0LCJpYXQiOjE2OTg4MzEwMDQsIm5iZiI6MTY5ODgzMTAwNCwiaXNzIjoiaHR0cHM6Ly90c3dpbC5jbG91ZGZsYXJlYWNjZXNzLmNvbSIsInR5cGUiOiJhcHAiLCJpZGVudGl0eV9ub25jZSI6InRiU3d2Tk5CeTkycjlVcWsiLCJzdWIiOiI2YzNiNzc0NS03ZjczLTViYjktOTczOS03MTgyM2I5ODExODIiLCJjb3VudHJ5IjoiSUwifQ.FxNh87nvyIicCW732go2RV7Q1Mxzwwq2gUXxpOQgx2S7vWpcx5dLlyl3CDvUDpSfzr9mnlNxAcBEVmPGIL64swR4KEaNS9dQUsXYwPYlnjNeuvHWMdrwNHpzcSxz-HanFHV_C5ujMqJMYScdv01Ub8KRodzovBvFrIqF4zeuZA40dwNgYb34-oNYfZZ61g52CQCiUSF0xoMA5ZZc2C63KVNl7ULS63pPAfeJgMKHgepMiXxgq__vK2ZrGjiY_kc4qhKwQVWzTzJiSHgAqdZCLNbd7lnnrBKFvqyen4gBhf7ykdQQj6gBCHeUQzV9Jz9Dn324PRsnH7XK2OkNeKDGDg"}
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
