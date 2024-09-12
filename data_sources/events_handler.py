from data_sources.utils import EventHandlerUtils
from data_sources.active_fence.active_fence_ingestion import ActiveFenceIngest
from data_sources.scooper.scooper_ingestion import ScooperIngestion


class EventHandler:

    def run(self, event):
        return self.route_data_to_object(event)

    @staticmethod
    def route_data_to_object(event):
        if event[EventHandlerUtils.EVENT_NAME] == "scooper" or "scooper" in event[EventHandlerUtils.EVENT_NAME]:
            obj = ScooperIngestion(event)
            obj.run()
            return obj.running_timestamp_id

        elif event[EventHandlerUtils.EVENT_NAME] == "activefence":
            ActiveFenceIngest(event).run()
        else:
            raise Exception("No event_name exists")



# if __name__ == "__main__":
    # with open('config_file_.json', 'r') as f:
    #     config_data = json.load(f)
    #     f.close()
    # obj = EventHandler()
    # obj.run({'event_name': 'scooper', 'test_env_status': True, 'bucket_name': 'data-omhds', 'input_file': 'test/scooper_imports/2024/09/12/_scooper.json'})

