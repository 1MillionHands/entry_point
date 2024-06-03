from utils import EventHandlerUtils
import boto3
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from table_objects.post import PostHandler

with open('DB_Manager_EP/config_file.json', 'r') as f:
    config_data = json.load(f)

# with open('./DB_Manager_EP/config_file.json', 'r') as f:
#     config_data = json.load(f)

client = boto3.client('lambda')


class EventHandler:

    def __init__(self):
        self.config_data = config_data
        retry_strategy = Retry(
            total=10,  # Maximum number of retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )
        self.adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)

    def run(self, event):
        print("success 2")
        # url = self.get_url(event[EventHandlerUtils.EVENT_NAME])
        # raw_data = self.get_api_data(url)
        # with open("mydata.txt") as f:
        #     temp = f.read()
        #     raw_data = json.loads(temp)
        #     raw_data = json.loads(raw_data)['results']
        # self.route_data_to_object(raw_data, event)

    def get_headers(self):
        return {"accept": "application/json",
                "Cookie": f"CF_Authorization={self.config_data['letBotsWorkToekn']}"}

    # def adjust_url_page_limit(self, url):
    #     return url + '&limit=' + str(EventHandlerUtils.page_size)

    def get_api_data(self, url, filters=None):
        """

        :param filters:
        :return:
        """

        # url = self.adjust_url_page_limit(url)
        headers = self.get_headers()
        response = self.session.get(url, headers=headers, timeout=50)
        print(url, '\n', headers)
        if response.status_code == EventHandlerUtils.SUCCESS_RESPONSE:
            # todo: add expections to check response payload to catch failures
            raw_data = response.text
            raw_data_js = json.loads(raw_data)['results']
            print(EventHandlerUtils.SUCCESS)
            return raw_data_js
        else:
            print("FAILED", response.status_code)

    def get_url(self, event_name):
        # page_size = self.config_data.get("page_size")
        return f'https://api.oct7.io/posts?sort=created_at.desc&limit={EventHandlerUtils.page_size}'

    @staticmethod
    def route_data_to_object(raw_data, event):
        if event[EventHandlerUtils.EVENT_NAME] == "get_posts":
            PostHandler(raw_data, event).run('run_from_api')

        elif event[EventHandlerUtils.EVENT_NAME] == "creator":
            pass

        else:
            raise Exception("No event_name exists")

    # @staticmethod
    # def validate_table_name(name):
    #     # todo: check if works well
    #     import inspect
    #     # Get all classes defined in the module
    #     all_classes = inspect.getmembers(tbls, inspect.isclass)
    #
    #     # Extract class names
    #     class_names = [cls[0] == name for cls in all_classes ]
    #     if any(class_names):
    #         return name
    #     else:
    #         raise ("the name of the table doesn't exist in the table_objects file (non of the classes contain that name)")

    # @repeat(every(config_data['schedule']['hours']).hours)
    # def get_posts(self):# chage to get_api_data()
    #     url = self.postApi.get_url()
    #     response = self.postApi.get_post_information(url)
    #     if response.status_code == EventHandlerUtils.SUCCESS_RESPONSE:
    #         # todo: add expections to check response payload to catch failures
    #         raw_posts = response.text
    #         posts = json.loads(raw_posts)['results']
    #         self.upload_to_db(posts)
    #         self.upload_to_s3(raw_posts)
    #         print(SUCCESS)
    #     else:
    #         print("FAILED", response.status_code)


if __name__ == "__main__":
    # with open('config_file.json', 'r') as f:
    #     config_data = json.load(f)
    #     f.close()
    obj = EventHandler()
    obj.run({'event_name': 'get_posts', 'env_status': 'dev', 's3_filename': 'post_temp.json'})
    # print(DbService(True).DATABASE_URL)
