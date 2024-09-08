from utils import EventHandlerUtils
from DB_Manager_EP.table_objects.post import PostHandler
from DB_Manager_EP.data_sources.active_fence.active_fence_ingestion import ActiveFenceIngest
from DB_Manager_EP.data_sources.scooper.scooper_ingestion import ScooperIngestion


class EventHandler:

    def run(self, event):
        return self.route_data_to_object(event)

    @staticmethod
    def route_data_to_object(event):
        if event[EventHandlerUtils.EVENT_NAME] == "get_posts":
            PostHandler(event).run('run_from_api')

        elif event[EventHandlerUtils.EVENT_NAME] == "activefence":
            ActiveFenceIngest(event).run()

        elif event[EventHandlerUtils.EVENT_NAME] == "scooper":
            obj = ScooperIngestion(event)
            obj.run()
            return obj

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
