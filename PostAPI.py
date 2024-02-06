import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class PostAPI:

    def __init__(self, config_data):
        self.config_data = config_data
        retry_strategy = Retry(
            total=10,  # Maximum number of retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        self.adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)

    def get_headers(self):
        pass

    def get_post_information(self, url):
        pass

    def get_url(self):
        pass
