from utils import EventHandlerUtils
import json
from table_objects.post import PostHandler
from DB_Manager_EP.data_sources.active_fence.active_fence_ingestion import ActiveFenceIngest

# with open('./DB_Manager_EP/config_file.json_', 'r') as f:
#   config_data = json.load(f)

# with open(r'C:\Users\yanir\PycharmProjects\oneMilion\entry_point\DB_Manager_EP\config_file_.json', 'r') as f:
#   config_data = json.load(f)
with open('config_file.json', 'r') as f:
    config_data = json.load(f)

class EventHandler:

    def run(self, event):
        self.route_data_to_object(event)

    @staticmethod
    def route_data_to_object(event):
        if event[EventHandlerUtils.EVENT_NAME] == "get_posts":
            PostHandler(event).run('run_from_api')

        elif event[EventHandlerUtils.EVENT_NAME] == "activefence":
            ActiveFenceIngest(event).run()

        elif event[EventHandlerUtils.EVENT_NAME] == "creator":
            pass
        else:
            raise Exception("No event_name exists")



# if __name__ == "__main__":
    # with open('config_file_.json', 'r') as f:
    #     config_data = json.load(f)
    #     f.close()
    # obj = EventHandler()
    # obj.run({'event_name': 'get_posts', 'env_status': True, 's3_filename': 'post_temp.json'})
    # print(DbService(True).DATABASE_URL)
