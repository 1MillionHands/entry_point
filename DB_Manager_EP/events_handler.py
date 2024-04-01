from utils import tbl_setting
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def get_object_headers():
    return json.loads(tbl_setting)


SUCCESS_RESPONSE = 200

SUCCESS = "SUCCESS"


class EventHandler:

    def __init__(self, config_data, table_objects_txt):
        self.config_data = config_data
        retry_strategy = Retry(
            total=10,  # Maximum number of retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        self.adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)
        self.table_objects_txt = self.validate_table_name(table_objects_txt)


    def run(self, url):

        # with open('raw_posts (1).json', 'rb') as f:
        #     raw_data = json.loads(f.read())["results"]
        #     f.close()

        raw_data = self.get_api_data(url)
        Post.run(data)

        # # print(raw_data)
        # # self.upload_to_s3(raw_data)
        # data = tbls.Post().match_headers(raw_data)
        # data = tbls.Post().transform_data(data)
        # self.upload_to_db(data)


    def route_to_lambda(self, data):
        if self.event_type == "post_scrapper":
            data = tbls.PostTest.transform_data(data)
    #         s3 trigger 4 lambdas
        else:
            pass
    #         self.event_type == "post_from_api":
    #     elif self.event_type == "creator":
    # #         trigger 2 lambdas
    #     else:
    #         end job


    def get_headers(self):
        return {"accept": "application/json",
                "Cookie": f"CF_Authorization={self.config_data['letBotsWorkToekn']}"}


    def adjust_url_page_limit(self, url):
        page_size = self.config_data.get("page_size")
        return url + '&limit=' + str(page_size)


    def get_api_data(self, url, filters = None):
        """

        :param filters:
        :return:
        """

        url = self.adjust_url_page_limit(url)
        headers = self.get_headers()
        response = self.session.get(url, headers=headers, timeout=50)
        if response.status_code == SUCCESS_RESPONSE:
            # todo: add expections to check response payload to catch failures
            raw_data = response.text
            raw_data_js = json.loads(raw_data)['results']
            print(SUCCESS)
            return raw_data_js
        else:
            print("FAILED", response.status_code)


    @staticmethod
    def validate_table_name(name):
        # todo: check if works well
        import inspect
        # Get all classes defined in the module
        all_classes = inspect.getmembers(tbls, inspect.isclass)

        # Extract class names
        class_names = [cls[0] == name for cls in all_classes ]
        if any(class_names):
            return name
        else:
            raise ("the name of the table doesn't exist in the table_objects file (non of the classes contain that name)")

    def get_url(self):
        page_size = self.config_data.get("page_size")
        return f'https://api.oct7.io/posts?sort=created_at.desc&limit={page_size}'

if __name__ == "__main__":
    with open('../config_file.json', 'r') as f:
        config_data = json.load(f)
        f.close()
    obj = EventHandler(config_data, "Post", "post_scrapper")
    obj.run(obj.get_url())
    # print(DbService(True).DATABASE_URL)

